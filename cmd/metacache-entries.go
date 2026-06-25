// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/pkg/v3/console"
)

// metaCacheEntry is an object or a directory within an unknown bucket.
type metaCacheEntry struct {
	// name is the full name of the object including prefixes
	name string
	// Metadata. If none is present it is not an object but only a prefix.
	// Entries without metadata will only be present in non-recursive scans.
	metadata []byte

	// cached contains the metadata if decoded.
	cached *xlMetaV2

	// Indicates the entry can be reused and only one reference to metadata is expected.
	reusable bool
}

// isDir reports whether the entry represents a synthetic prefix directory.
//
// Directory entries have no object metadata and their name ends with "/".
func (e metaCacheEntry) isDir() bool {
	return len(e.metadata) == 0 && strings.HasSuffix(e.name, slashSeparator)
}

// isObject returns if the entry is representing an object.
func (e metaCacheEntry) isObject() bool {
	return len(e.metadata) > 0
}

// isObjectDir returns whether the entry represents an object whose name ends with "/".
func (e metaCacheEntry) isObjectDir() bool {
	return len(e.metadata) > 0 && strings.HasSuffix(e.name, slashSeparator)
}

// hasPrefix returns whether an entry has a specific prefix
func (e metaCacheEntry) hasPrefix(s string) bool {
	return strings.HasPrefix(e.name, s)
}

// matches returns if the entries have the same versions.
// If strict is false we allow signatures to mismatch.
func (e *metaCacheEntry) matches(other *metaCacheEntry, strict bool) (prefer *metaCacheEntry, matches bool) {
	if e == nil && other == nil {
		return nil, true
	}
	if e == nil {
		return other, false
	}
	if other == nil {
		return e, false
	}

	// Name should match...
	if e.name != other.name {
		if e.name < other.name {
			return e, false
		}
		return other, false
	}

	// Directories/prefixes do not have xlmeta.
	// If both are synthetic dirs, they match.
	// If only one is synthetic dir, prefer the real object.
	if e.isDir() || other.isDir() {
		switch {
		case e.isDir() && other.isDir():
			return e, true
		case e.isDir():
			return other, false
		default:
			return e, false
		}
	}

	eVers, eErr := e.xlmeta()
	oVers, oErr := other.xlmeta()
	if eErr != nil || oErr != nil {
		return nil, false
	}

	// check both fileInfo's have same number of versions, if not skip
	// the `other` entry.
	if len(eVers.versions) != len(oVers.versions) {
		eTime := eVers.latestModtime()
		oTime := oVers.latestModtime()
		if !eTime.Equal(oTime) {
			if eTime.After(oTime) {
				return e, false
			}
			return other, false
		}
		// Tiebreak on version count.
		if len(eVers.versions) > len(oVers.versions) {
			return e, false
		}
		return other, false
	}

	// Check if each version matches...
	//TODO: check if this versions come ordered, otherwise it can report false mismatches.
	for i, eVer := range eVers.versions {
		oVer := oVers.versions[i]
		if eVer.header != oVer.header {
			if eVer.header.hasEC() != oVer.header.hasEC() {
				// One version has EC and the other doesn't - may have been written later.
				// Compare without considering EC.
				a, b := eVer.header, oVer.header
				a.EcN, a.EcM = 0, 0
				b.EcN, b.EcM = 0, 0
				if a == b {
					continue
				}
			}
			if !strict && eVer.header.matchesNotStrict(oVer.header) {
				if prefer == nil {
					if eVer.header.sortsBefore(oVer.header) {
						prefer = e
					} else {
						prefer = other
					}
				}
				continue
			}
			if prefer != nil {
				return prefer, false
			}

			if eVer.header.sortsBefore(oVer.header) {
				return e, false
			}
			return other, false
		}
	}
	// If we match, return e
	if prefer == nil {
		prefer = e
	}
	return prefer, true
}

// isInDir returns whether the entry is directly inside dir when considering separator.
//
// dir is expected to be either empty for root, or normalized to end with separator.
func (e metaCacheEntry) isInDir(dir, separator string) bool {
	if separator == "" {
		return false
	}

	// safer if callers may pass dir without trailing /
	if dir != "" && !strings.HasSuffix(dir, separator) {
		dir += separator
	}

	if dir == "" {
		idx := strings.Index(e.name, separator)
		return idx == -1 || idx == len(e.name)-len(separator)
	}

	if !strings.HasPrefix(e.name, dir) {
		return false
	}

	ext := strings.TrimPrefix(e.name, dir)
	idx := strings.Index(ext, separator)

	return idx == -1 || idx == len(ext)-len(separator)
}

// isLatestDeletemarker returns whether the latest version is a delete marker.
// If metadata is NOT versioned false will always be returned.
// If v2 and UNABLE to load metadata true will be returned.
func (e *metaCacheEntry) isLatestDeletemarker() bool {
	if e.cached != nil {
		if len(e.cached.versions) == 0 {
			return true
		}
		return e.cached.versions[0].header.Type == DeleteType
	}
	if !isXL2V1Format(e.metadata) {
		return false
	}
	// stricter around isIndexedMetaV2() errors
	meta, _, err := isIndexedMetaV2(e.metadata)
	if err != nil {
		return true
	}
	if meta != nil {
		return meta.IsLatestDeleteMarker()
	}
	// Fall back...
	xlMeta, err := e.xlmeta()
	if err != nil || len(xlMeta.versions) == 0 {
		return true
	}
	return xlMeta.versions[0].header.Type == DeleteType
}

// isAllFreeVersions returns if all objects are free versions.
// If metadata is NOT versioned false will always be returned.
// If v2 and UNABLE to load metadata true will be returned.
func (e *metaCacheEntry) isAllFreeVersions() bool {
	if e.cached != nil {
		if len(e.cached.versions) == 0 {
			return true
		}
		for _, v := range e.cached.versions {
			if !v.header.FreeVersion() {
				return false
			}
		}
		return true
	}

	if !isXL2V1Format(e.metadata) {
		return false
	}
	meta, _, err := isIndexedMetaV2(e.metadata)
	if err != nil {
		return true
	}
	if meta != nil {
		return meta.AllHidden(false)
	}

	// Fall back...
	xlMeta, err := e.xlmeta()
	if err != nil || len(xlMeta.versions) == 0 {
		return true
	}
	// still can be e.cached may still be nil
	for _, v := range xlMeta.versions {
		if !v.header.FreeVersion() {
			return false
		}
	}
	return true
}

// fileInfo returns the decoded metadata.
// If entry is a directory it is returned as that.
// If versioned the latest version will be returned.
func (e *metaCacheEntry) fileInfo(bucket string) (FileInfo, error) {
	if e.isDir() {
		return FileInfo{
			Volume: bucket,
			Name:   e.name,
			Mode:   uint32(os.ModeDir),
		}, nil
	}
	if e.cached != nil {
		if len(e.cached.versions) == 0 {
			// This special case is needed to handle xlMeta.versions == 0
			return FileInfo{
				Volume:   bucket,
				Name:     e.name,
				Deleted:  true,
				IsLatest: true,
				ModTime:  timeSentinel1970,
			}, nil
		}
		return e.cached.ToFileInfo(bucket, e.name, "", false, true)
	}

	// protect against malformed entries
	if len(e.metadata) == 0 {
		return FileInfo{}, fmt.Errorf("metaCacheEntry: no metadata for non-directory entry %q", e.name)
	}

	return getFileInfo(e.metadata, bucket, e.name, "", fileInfoOpts{})
}

// xlmeta returns the decoded metadata.
// This should not be called on directories.
func (e *metaCacheEntry) xlmeta() (*xlMetaV2, error) {
	if e.isDir() {
		return nil, errFileNotFound
	}
	if e.cached != nil {
		return e.cached, nil
	}
	if len(e.metadata) == 0 {
		// Only happens if the entry is not found or malformed.
		return nil, errFileNotFound
	}
	var xl xlMetaV2
	if err := xl.LoadOrConvert(e.metadata); err != nil {
		return nil, err
	}
	e.cached = &xl
	return e.cached, nil
}

// fileInfoVersions returns the metadata as FileInfoVersions.
// If entry is a directory it is returned as that.
func (e *metaCacheEntry) fileInfoVersions(bucket string) (FileInfoVersions, error) {
	if e.isDir() {
		return FileInfoVersions{
			Volume: bucket,
			Name:   e.name,
			Versions: []FileInfo{
				{
					Volume: bucket,
					Name:   e.name,
					Mode:   uint32(os.ModeDir),
				},
			},
		}, nil
	}
	// for non-directory entry with empty metadata
	if len(e.metadata) == 0 {
		return FileInfoVersions{}, errFileNotFound
	}
	// Too small gains to reuse cache here.
	return getFileInfoVersions(e.metadata, bucket, e.name, true)
}

// metaCacheEntries is a slice of metacache entries.
type metaCacheEntries []metaCacheEntry

// less function for sorting.
func (m metaCacheEntries) less(i, j int) bool {
	return m[i].name < m[j].name
}

// sort entries by name.
// m is sorted and a sorted metadata object is returned.
// Changes to m will also be reflected in the returned object.
func (m metaCacheEntries) sort() metaCacheEntriesSorted {
	if m.isSorted() {
		return metaCacheEntriesSorted{o: m}
	}
	sort.Slice(m, m.less)
	return metaCacheEntriesSorted{o: m}
}

// isSorted returns whether the objects are sorted.
// This is usually orders of magnitude faster than actually sorting.
func (m metaCacheEntries) isSorted() bool {
	return sort.SliceIsSorted(m, m.less)
}

// shallowClone will create a shallow clone of the array objects,
// but object metadata will not be cloned.
func (m metaCacheEntries) shallowClone() metaCacheEntries {
	dst := make(metaCacheEntries, len(m))
	copy(dst, m)
	return dst
}

type metadataResolutionParams struct {
	dirQuorum int // Number if disks needed for a directory to 'exist'.
	objQuorum int // Number of disks needed for an object to 'exist'.

	// An optimization request only an 'n' amount of versions from xl.meta
	// to avoid resolving all versions to figure out the latest 'version'
	// for ListObjects, ListObjectsV2
	requestedVersions int

	bucket string // Name of the bucket. Used for generating cached fileinfo.
	strict bool   // Versions must match exactly, including all metadata.

	// Reusable slice for resolution
	candidates [][]xlMetaV2ShallowVersion
}

// resolve multiple entries.
// entries are resolved by majority, then if tied by mod-time and versions.
// Names must match on all entries in m.
func (m metaCacheEntries) resolve(r *metadataResolutionParams) (selected *metaCacheEntry, ok bool) {
	if len(m) == 0 || r == nil {
		return nil, false
	}

	if cap(r.candidates) < len(m) {
		r.candidates = make([][]xlMetaV2ShallowVersion, 0, len(m))
	}
	r.candidates = r.candidates[:0]

	// Separate object and dir selection
	var dirSelected *metaCacheEntry
	var objSelected *metaCacheEntry

	dirExists := 0
	objsAgree := 0
	objsValid := 0

	for i := range m {
		entry := &m[i]
		// Empty entry
		if entry.name == "" {
			continue
		}

		if entry.isDir() {
			dirExists++
			if dirSelected == nil {
				dirSelected = entry
			}
			continue
		}

		// Get new entry metadata,
		// shallow decode.
		xl, err := entry.xlmeta()
		if err != nil {
			continue
		}
		objsValid++

		// Add all valid to candidates.
		r.candidates = append(r.candidates, xl.versions)

		// We select the first object we find as a candidate and see if all match that.
		// This is to quickly identify if all agree.
		if objSelected == nil {
			objSelected = entry
			objsAgree = 1
			continue
		}
		// Names match, check meta...
		if prefer, matches := entry.matches(objSelected, r.strict); matches {
			objSelected = prefer
			objsAgree++
		}
	}

	// Prefer real object metadata if object quorum exists.
	if objsValid >= r.objQuorum {
		if objSelected == nil || objSelected.cached == nil {
			return nil, false
		}
		if objsAgree == objsValid {
			return objSelected, true
		}

		merged := &metaCacheEntry{
			name:     objSelected.name,
			reusable: true,
			cached:   &xlMetaV2{metaV: objSelected.cached.metaV},
		}
		merged.cached.versions = mergeXLV2Versions(r.objQuorum, r.strict, r.requestedVersions, r.candidates...)

		if len(merged.cached.versions) == 0 {
			return nil, false
		}

		buf := metaDataPoolGet()
		var err error
		// it may lose the pooled buffer if AppendTo fails
		merged.metadata, err = merged.cached.AppendTo(buf)
		if err != nil {
			metaDataPoolPut(buf)
			bugLogIf(context.Background(), err)
			return nil, false
		}

		return merged, true
	}

	// Fall back to synthetic directory only if object quorum was not reached.
	if dirSelected != nil && dirExists >= r.dirQuorum {
		return dirSelected, true
	}

	return nil, false
}

// firstFound returns the first found and the number of set entries.
func (m metaCacheEntries) firstFound() (first *metaCacheEntry, n int) {
	// avoids copying each metaCacheEntry during range iteration
	for i := range m {
		if m[i].name == "" {
			continue
		}
		n++
		if first == nil {
			first = &m[i]
		}
	}

	return first, n
}

// names will return all names in order.
// Since this allocates it should not be used in critical functions.
func (m metaCacheEntries) names() []string {
	res := make([]string, 0, len(m))
	for _, obj := range m {
		res = append(res, obj.name)
	}
	return res
}

// metaCacheEntriesSorted contains metacache entries that are sorted.
type metaCacheEntriesSorted struct {
	o metaCacheEntries
	// list id is not serialized
	listID string
	// Reuse buffers
	reuse bool
	// Contain the last skipped object after an ILM expiry evaluation
	lastSkippedEntry string
}

// shallowClone will create a shallow clone of the array objects,
// but object metadata will not be cloned.
func (m metaCacheEntriesSorted) shallowClone() metaCacheEntriesSorted {
	// We have value receiver so we already have a copy.
	m.o = m.o.shallowClone()
	return m
}

// fileInfoVersions converts the metadata to FileInfoVersions where possible.
// Metadata that cannot be decoded is skipped.
func (m *metaCacheEntriesSorted) fileInfoVersions(bucket, prefix, delimiter, afterV string) (versions []ObjectInfo) {
	versions = make([]ObjectInfo, 0, m.len())
	prevPrefix := ""
	vcfg, _ := globalBucketVersioningSys.Get(bucket)

	for i := range m.o {
		// avoid copying
		entry := &m.o[i]

		if prefix != "" && !strings.HasPrefix(entry.name, prefix) {
			continue
		}

		if entry.isObject() {
			if delimiter != "" {
				rest := strings.TrimPrefix(entry.name, prefix)

				idx := strings.Index(rest, delimiter)
				if idx >= 0 {
					idx = len(prefix) + idx + len(delimiter)
					currPrefix := entry.name[:idx]
					if currPrefix == prevPrefix {
						continue
					}

					prevPrefix = currPrefix
					versions = append(versions, ObjectInfo{
						IsDir:  true,
						Bucket: bucket,
						Name:   currPrefix,
					})

					continue
				}
			}

			fiv, err := entry.fileInfoVersions(bucket)
			if err != nil {
				continue
			}

			fiVersions := fiv.Versions
			if afterV != "" {
				vidMarkerIdx := fiv.findVersionIndex(afterV)
				if vidMarkerIdx >= 0 {
					fiVersions = fiVersions[vidMarkerIdx+1:]
				}
				afterV = ""
			}

			versioned := vcfg != nil && vcfg.Versioned(entry.name)

			for _, version := range fiVersions {
				if !version.VersionPurgeStatus().Empty() {
					continue
				}
				versions = append(versions, version.ToObjectInfo(bucket, entry.name, versioned))
			}

			continue
		}

		if entry.isDir() {
			if delimiter == "" {
				continue
			}
			rest := strings.TrimPrefix(entry.name, prefix)

			idx := strings.Index(rest, delimiter)
			if idx < 0 {
				continue
			}

			idx = len(prefix) + idx + len(delimiter)
			currPrefix := entry.name[:idx]
			if currPrefix == prevPrefix {
				continue
			}

			prevPrefix = currPrefix
			versions = append(versions, ObjectInfo{
				IsDir:  true,
				Bucket: bucket,
				Name:   currPrefix,
			})
		}
	}

	return versions
}

// fileInfos converts the metadata to ObjectInfo where possible.
// Metadata that cannot be decoded is skipped.
func (m *metaCacheEntriesSorted) fileInfos(bucket, prefix, delimiter string) (objects []ObjectInfo) {
	objects = make([]ObjectInfo, 0, m.len())
	prevPrefix := ""

	vcfg, _ := globalBucketVersioningSys.Get(bucket)

	for i := range m.o {
		// avoid copy
		entry := &m.o[i]

		if prefix != "" && !strings.HasPrefix(entry.name, prefix) {
			continue
		}
		if entry.isObject() {
			if delimiter != "" {
				rest := strings.TrimPrefix(entry.name, prefix)
				idx := strings.Index(rest, delimiter)
				if idx >= 0 {
					idx = len(prefix) + idx + len(delimiter)
					currPrefix := entry.name[:idx]
					if currPrefix == prevPrefix {
						continue
					}

					prevPrefix = currPrefix
					objects = append(objects, ObjectInfo{
						IsDir:  true,
						Bucket: bucket,
						Name:   currPrefix,
					})
					continue
				}
			}

			fi, err := entry.fileInfo(bucket)
			if err == nil && fi.VersionPurgeStatus().Empty() {
				versioned := vcfg != nil && vcfg.Versioned(entry.name)
				objects = append(objects, fi.ToObjectInfo(bucket, entry.name, versioned))
			}
			continue
		}
		if entry.isDir() {
			if delimiter == "" {
				continue
			}
			rest := strings.TrimPrefix(entry.name, prefix)
			idx := strings.Index(rest, delimiter)
			if idx < 0 {
				continue
			}
			idx = len(prefix) + idx + len(delimiter)
			currPrefix := entry.name[:idx]
			if currPrefix == prevPrefix {
				continue
			}
			prevPrefix = currPrefix
			objects = append(objects, ObjectInfo{
				IsDir:  true,
				Bucket: bucket,
				Name:   currPrefix,
			})
		}
	}

	return objects
}

// forwardTo will truncate m so only entries that are s or after are in the list.
//
// Requires m.o to be sorted by name.
func (m *metaCacheEntriesSorted) forwardTo(s string) {
	if s == "" {
		return
	}
	idx := sort.Search(len(m.o), func(i int) bool {
		return m.o[i].name >= s
	})
	for i := range m.o[:idx] {
		if m.reuse && cap(m.o[i].metadata) >= metaDataReadDefault {
			metaDataPoolPut(m.o[i].metadata)
		}
		// Clear references so skipped entries do not stay alive through
		// the backing array.
		m.o[i] = metaCacheEntry{}
	}

	m.o = m.o[idx:]
}

// forwardPast will truncate m so only entries that are after s is in the list.
func (m *metaCacheEntriesSorted) forwardPast(s string) {
	if s == "" {
		return
	}
	idx := sort.Search(len(m.o), func(i int) bool {
		return m.o[i].name > s
	})
	if m.reuse {
		for i, entry := range m.o[:idx] {
			metaDataPoolPut(entry.metadata)
			m.o[i].metadata = nil
		}
	}
	m.o = m.o[idx:]
}

// mergeEntryChannels will merge sorted entries from in and return them sorted on out.
// To signify no more results are on an input channel, close it.
// The output channel will be closed when all inputs are emptied.
// If file names are equal, metadata is merged/resolved.
// The entry not chosen will be discarded.
// If the context is canceled the function will return the error,
// otherwise the function will return nil.
//
// Each input channel must produce entries sorted by name.
func mergeEntryChannels(ctx context.Context, in []chan metaCacheEntry, out chan<- metaCacheEntry, readQuorum int) error {
	defer xioutil.SafeClose(out)

	ctxDone := ctx.Done()

	releaseEntry := func(e *metaCacheEntry) {
		if e == nil {
			return
		}
		if e.reusable && e.metadata != nil {
			metaDataPoolPut(e.metadata)
		}
		e.metadata = nil
		e.cached = nil
	}

	// Do not use path.Clean here. Object keys may legally contain repeated
	// slashes or "." / ".." segments. We only want to treat "name" and "name/"
	// as the same logical listing position for dir/object conflict handling.
	sameListedName := func(a, b string) bool {
		if a == b {
			return true
		}
		return strings.TrimSuffix(a, slashSeparator) == strings.TrimSuffix(b, slashSeparator)
	}

	if len(in) == 0 {
		return nil
	}

	// Simple forwarder for one input.
	if len(in) == 1 {
		if in[0] == nil {
			return nil
		}
		for {
			select {
			case <-ctxDone:
				return ctx.Err()
			case v, ok := <-in[0]:
				if !ok {
					return nil
				}
				select {
				case <-ctxDone:
					releaseEntry(&v)
					return ctx.Err()
				case out <- v:
				}
			}
		}
	}

	top := make([]*metaCacheEntry, len(in))
	done := make([]bool, len(in))
	nDone := 0

	releaseTop := func(idx int) {
		releaseEntry(top[idx])
		top[idx] = nil
	}

	defer func() {
		for i := range top {
			releaseTop(i)
		}
	}()

	readTop := func(idx int) error {
		if done[idx] {
			top[idx] = nil
			return nil
		}

		if in[idx] == nil {
			done[idx] = true
			top[idx] = nil
			nDone++
			return nil
		}

		select {
		case <-ctxDone:
			return ctx.Err()
		case entry, ok := <-in[idx]:
			if !ok {
				top[idx] = nil
				done[idx] = true
				nDone++
				return nil
			}
			top[idx] = &entry
			return nil
		}
	}

	discardAndReadTop := func(idx int) error {
		releaseTop(idx)
		return readTop(idx)
	}

	// Populate initial heads.
	for i := range in {
		if err := readTop(i); err != nil {
			return err
		}
	}
	last := ""
	toMerge := make([]int, 0, len(in)-1)

	for {
		if nDone == len(in) {
			return nil
		}
		var best *metaCacheEntry
		bestIdx := -1
		toMerge = toMerge[:0]

		for i, other := range top {
			if other == nil {
				continue
			}
			if best == nil {
				best = other
				bestIdx = i
				continue
			}
			if sameListedName(best.name, other.name) {
				// We may have a synthetic directory and an object with the
				// same logical listing name. Drop the synthetic directory.
				dirMatches := best.isDir() == other.isDir()
				suffixMatches := strings.HasSuffix(best.name, slashSeparator) == strings.HasSuffix(other.name, slashSeparator)

				// Same type and same slash suffix: resolve/merge them.
				if dirMatches && suffixMatches {
					toMerge = append(toMerge, i)
					continue
				}

				if !dirMatches {
					if other.isDir() {
						if serverDebugLog {
							console.Debugln("mergeEntryChannels: discarding directory", other.name, "for object", best.name)
						}
						if err := discardAndReadTop(i); err != nil {
							return err
						}
						continue
					}
					if serverDebugLog {
						console.Debugln("mergeEntryChannels: discarding directory", best.name, "for object", other.name)
					}
					toMerge = toMerge[:0]
					best = other
					bestIdx = i
					continue
				}

				// Same dir/object type but different slash suffix.
				// Leave normal lexical ordering to decide.
			}
			if best.name > other.name {
				toMerge = toMerge[:0]
				best = other
				bestIdx = i
			}
		}
		if best == nil {
			return nil
		}

		if len(toMerge) > 0 {
			// Duplicate synthetic directories: keep one, discard the rest.
			if best.isDir() {
				for _, idx := range toMerge {
					if err := discardAndReadTop(idx); err != nil {
						return err
					}
				}
				toMerge = toMerge[:0]
			} else {
				versions := make([][]xlMetaV2ShallowVersion, 0, len(toMerge)+1)
				xl, err := best.xlmeta()
				if err == nil {
					versions = append(versions, xl.versions)
				} else {
					xl = nil
				}
				for _, idx := range toMerge {
					other := top[idx]
					if other == nil {
						continue
					}

					xlOther, err := other.xlmeta()
					if err != nil {
						if err := discardAndReadTop(idx); err != nil {
							return err
						}
						continue
					}
					if xl == nil {
						// Current best was invalid. Discard it and promote
						// the first valid duplicate.
						if err := discardAndReadTop(bestIdx); err != nil {
							return err
						}
						best = other
						bestIdx = idx
						xl = xlOther
					} else {
						if err := discardAndReadTop(idx); err != nil {
							return err
						}
					}
					versions = append(versions, xlOther.versions)
				}

				if xl == nil || len(versions) == 0 {
					if err := discardAndReadTop(bestIdx); err != nil {
						return err
					}
					continue
				}

				mergedVersions := mergeXLV2Versions(readQuorum, true, 0, versions...)
				// no longer emits best when duplicate metadata merge fails to satisfy quorum
				if len(mergedVersions) == 0 {
					if err := discardAndReadTop(bestIdx); err != nil {
						return err
					}
					continue
				}

				xl.versions = mergedVersions

				buf := metaDataPoolGet()
				meta, err := xl.AppendTo(buf)
				if err != nil {
					metaDataPoolPut(buf)
					bugLogIf(context.Background(), err)

					if err := discardAndReadTop(bestIdx); err != nil {
						return err
					}
					continue
				}
				if best.reusable && best.metadata != nil {
					metaDataPoolPut(best.metadata)
				}

				best.metadata = meta
				best.cached = xl
				best.reusable = true
			}
		}

		if best.name > last {
			select {
			case <-ctxDone:
				return ctx.Err()
			case out <- *best:
				last = best.name

				// Ownership of best.metadata has moved to the receiver.
				// Clear top before readTop so deferred cleanup does not
				// return metadata that was already sent.
				top[bestIdx] = nil
			}
			if err := readTop(bestIdx); err != nil {
				return err
			}
			continue
		}

		if serverDebugLog {
			console.Debugln("mergeEntryChannels: discarding duplicate", best.name, "<=", last)
		}

		if err := discardAndReadTop(bestIdx); err != nil {
			return err
		}
	}
}

// merge will merge other into m.
// If the same entry exists in both and metadata matches, only one is added.
// If names are equal but metadata differs, the entry from m is placed first.
// Operation time is expected to be O(n+m), or O(limit) if limit > 0.
func (m *metaCacheEntriesSorted) merge(other metaCacheEntriesSorted, limit int) {
	if limit == 0 {
		m.o = nil
		return
	}

	a := m.entries()
	b := other.entries()
	capHint := len(a) + len(b)
	if limit > 0 && capHint > limit {
		capHint = limit
	}

	merged := make(metaCacheEntries, 0, capHint)
	appendOne := func(entry metaCacheEntry) bool {
		if limit > 0 && len(merged) >= limit {
			return false
		}
		merged = append(merged, entry)
		return true
	}
	for len(a) > 0 && len(b) > 0 {
		if limit > 0 && len(merged) >= limit {
			break
		}
		switch {
		case a[0].name == b[0].name:
			if bytes.Equal(a[0].metadata, b[0].metadata) {
				// Same entry, discard one.
				if !appendOne(a[0]) {
					break
				}
				a = a[1:]
				b = b[1:]
				continue
			}
			// Same name, different metadata.
			// Preserve m first, as documented.
			if !appendOne(a[0]) {
				break
			}
			a = a[1:]
		case a[0].name < b[0].name:
			if !appendOne(a[0]) {
				break
			}
			a = a[1:]
		default:
			if !appendOne(b[0]) {
				break
			}
			b = b[1:]
		}
	}
	for len(a) > 0 && (limit < 0 || len(merged) < limit) {
		merged = append(merged, a[0])
		a = a[1:]
	}
	for len(b) > 0 && (limit < 0 || len(merged) < limit) {
		merged = append(merged, b[0])
		b = b[1:]
	}
	m.o = merged
}

// filterPrefix will filter m to only contain entries with the specified prefix.
//
// Requires m.o to be sorted by name.
func (m *metaCacheEntriesSorted) filterPrefix(s string) {
	if m == nil || s == "" {
		return
	}

	m.forwardTo(s)
	for i := range m.o {
		if m.o[i].hasPrefix(s) {
			continue
		}
		if m.reuse {
			for j := i; j < len(m.o); j++ {
				if cap(m.o[j].metadata) >= metaDataReadDefault {
					metaDataPoolPut(m.o[j].metadata)
				}
				m.o[j] = metaCacheEntry{}
			}
		} else {
			for j := i; j < len(m.o); j++ {
				m.o[j] = metaCacheEntry{}
			}
		}
		m.o = m.o[:i]
		return
	}
}

// filterObjectsOnly will remove prefix directories.
// Order is preserved, but the underlying slice is modified.
func (m *metaCacheEntriesSorted) filterObjectsOnly() {
	dst := m.o[:0]
	for i := range m.o {
		if !m.o[i].isDir() {
			dst = append(dst, m.o[i])
		}
	}
	// Clear the tail so removed entries do not remain referenced
	// by the underlying array.
	for i := len(dst); i < len(m.o); i++ {
		m.o[i] = metaCacheEntry{}
	}
	m.o = dst
}

// filterPrefixesOnly will remove objects.
// Order is preserved, but the underlying slice is modified.
func (m *metaCacheEntriesSorted) filterPrefixesOnly() {
	dst := m.o[:0]
	for i := range m.o {
		if m.o[i].isDir() {
			dst = append(dst, m.o[i])
			continue
		}
		if m.reuse && cap(m.o[i].metadata) >= metaDataReadDefault {
			metaDataPoolPut(m.o[i].metadata)
		}
	}
	// Clear the tail so removed entries do not stay referenced
	// by the underlying array.
	for i := len(dst); i < len(m.o); i++ {
		m.o[i] = metaCacheEntry{}
	}
	m.o = dst
}

// filterRecursiveEntries will keep entries only with the prefix that doesn't contain separator.
// This can be used to remove recursive listings.
// To return root elements only set prefix to an empty string.
// Order is preserved, but the underlying slice is modified.
func (m *metaCacheEntriesSorted) filterRecursiveEntries(prefix, separator string) {
	if separator == "" {
		return
	}
	if separator == "" {
		return
	}

	if prefix != "" {
		m.forwardTo(prefix)
	}

	dst := m.o[:0]

	for i := range m.o {
		name := m.o[i].name

		if prefix != "" {
			if !strings.HasPrefix(name, prefix) {
				break
			}
			ext := strings.TrimPrefix(name, prefix)
			if ext == "" || !strings.Contains(ext, separator) {
				dst = append(dst, m.o[i])
				continue
			}
		} else {
			if !strings.Contains(name, separator) {
				dst = append(dst, m.o[i])
				continue
			}
		}
		if m.reuse && cap(m.o[i].metadata) >= metaDataReadDefault {
			metaDataPoolPut(m.o[i].metadata)
		}
	}

	for i := len(dst); i < len(m.o); i++ {
		// Clear references held by the backing array.
		m.o[i] = metaCacheEntry{}
	}

	m.o = dst
}

// truncate the number of entries to maximum n.
func (m *metaCacheEntriesSorted) truncate(n int) {
	if m == nil {
		return
	}
	if n < 0 {
		n = 0
	}
	if len(m.o) <= n {
		return
	}
	for i := n; i < len(m.o); i++ {
		if m.reuse && cap(m.o[i].metadata) >= metaDataReadDefault {
			metaDataPoolPut(m.o[i].metadata)
		}
		// Clear references held by the backing array.
		m.o[i] = metaCacheEntry{}
	}
	m.o = m.o[:n]
}

// len returns the number of objects and prefix dirs in m.
func (m *metaCacheEntriesSorted) len() int {
	if m == nil {
		return 0
	}
	return len(m.o)
}

// entries returns the underlying objects as is currently represented.
func (m *metaCacheEntriesSorted) entries() metaCacheEntries {
	if m == nil {
		return nil
	}
	return m.o
}
