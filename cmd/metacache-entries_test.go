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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

func Test_metaCacheEntries_sort(t *testing.T) {
	entries := loadMetacacheSampleEntries(t)

	o := entries.entries()
	if !o.isSorted() {
		t.Fatal("Expected sorted objects")
	}

	// Swap first and last
	o[0], o[len(o)-1] = o[len(o)-1], o[0]
	if o.isSorted() {
		t.Fatal("Expected unsorted objects")
	}

	sorted := o.sort()
	if !o.isSorted() {
		t.Fatal("Expected sorted o objects")
	}
	if !sorted.entries().isSorted() {
		t.Fatal("Expected sorted wrapped objects")
	}
	want := loadMetacacheSampleNames
	for i, got := range o {
		if got.name != want[i] {
			t.Errorf("entry %d, want %q, got %q", i, want[i], got.name)
		}
	}
}

func Test_metaCacheEntries_forwardTo(t *testing.T) {
	org := loadMetacacheSampleEntries(t)
	entries := org
	want := []string{"src/compress/zlib/reader_test.go", "src/compress/zlib/writer.go", "src/compress/zlib/writer_test.go"}
	entries.forwardTo("src/compress/zlib/reader_test.go")
	got := entries.entries().names()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got unexpected result: %#v", got)
	}

	// Try with prefix
	entries = org
	entries.forwardTo("src/compress/zlib/reader_t")
	got = entries.entries().names()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_merge(t *testing.T) {
	org := loadMetacacheSampleEntries(t)
	a, b := org.shallowClone(), org.shallowClone()
	be := b.entries()
	for i := range be {
		//  Modify b so it isn't deduplicated.
		be[i].metadata = []byte("something-else")
	}
	// Merge b into a
	a.merge(b, -1)
	//nolint:gocritic
	want := append(loadMetacacheSampleNames, loadMetacacheSampleNames...)
	sort.Strings(want)
	got := a.entries().names()
	if len(got) != len(want) {
		t.Errorf("unexpected count, want %v, got %v", len(want), len(got))
	}

	for i, name := range got {
		if want[i] != name {
			t.Errorf("unexpected name, want %q, got %q", want[i], name)
		}
	}
}

func Test_metaCacheEntries_filterObjects(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	data.filterObjectsOnly()
	got := data.entries().names()
	want := []string{"src/compress/bzip2/bit_reader.go", "src/compress/bzip2/bzip2.go", "src/compress/bzip2/bzip2_test.go", "src/compress/bzip2/huffman.go", "src/compress/bzip2/move_to_front.go", "src/compress/bzip2/testdata/Isaac.Newton-Opticks.txt.bz2", "src/compress/bzip2/testdata/e.txt.bz2", "src/compress/bzip2/testdata/fail-issue5747.bz2", "src/compress/bzip2/testdata/pass-random1.bin", "src/compress/bzip2/testdata/pass-random1.bz2", "src/compress/bzip2/testdata/pass-random2.bin", "src/compress/bzip2/testdata/pass-random2.bz2", "src/compress/bzip2/testdata/pass-sawtooth.bz2", "src/compress/bzip2/testdata/random.data.bz2", "src/compress/flate/deflate.go", "src/compress/flate/deflate_test.go", "src/compress/flate/deflatefast.go", "src/compress/flate/dict_decoder.go", "src/compress/flate/dict_decoder_test.go", "src/compress/flate/example_test.go", "src/compress/flate/flate_test.go", "src/compress/flate/huffman_bit_writer.go", "src/compress/flate/huffman_bit_writer_test.go", "src/compress/flate/huffman_code.go", "src/compress/flate/inflate.go", "src/compress/flate/inflate_test.go", "src/compress/flate/reader_test.go", "src/compress/flate/testdata/huffman-null-max.dyn.expect", "src/compress/flate/testdata/huffman-null-max.dyn.expect-noinput", "src/compress/flate/testdata/huffman-null-max.golden", "src/compress/flate/testdata/huffman-null-max.in", "src/compress/flate/testdata/huffman-null-max.wb.expect", "src/compress/flate/testdata/huffman-null-max.wb.expect-noinput", "src/compress/flate/testdata/huffman-pi.dyn.expect", "src/compress/flate/testdata/huffman-pi.dyn.expect-noinput", "src/compress/flate/testdata/huffman-pi.golden", "src/compress/flate/testdata/huffman-pi.in", "src/compress/flate/testdata/huffman-pi.wb.expect", "src/compress/flate/testdata/huffman-pi.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-1k.dyn.expect", "src/compress/flate/testdata/huffman-rand-1k.dyn.expect-noinput", "src/compress/flate/testdata/huffman-rand-1k.golden", "src/compress/flate/testdata/huffman-rand-1k.in", "src/compress/flate/testdata/huffman-rand-1k.wb.expect", "src/compress/flate/testdata/huffman-rand-1k.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-limit.dyn.expect", "src/compress/flate/testdata/huffman-rand-limit.dyn.expect-noinput", "src/compress/flate/testdata/huffman-rand-limit.golden", "src/compress/flate/testdata/huffman-rand-limit.in", "src/compress/flate/testdata/huffman-rand-limit.wb.expect", "src/compress/flate/testdata/huffman-rand-limit.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-max.golden", "src/compress/flate/testdata/huffman-rand-max.in", "src/compress/flate/testdata/huffman-shifts.dyn.expect", "src/compress/flate/testdata/huffman-shifts.dyn.expect-noinput", "src/compress/flate/testdata/huffman-shifts.golden", "src/compress/flate/testdata/huffman-shifts.in", "src/compress/flate/testdata/huffman-shifts.wb.expect", "src/compress/flate/testdata/huffman-shifts.wb.expect-noinput", "src/compress/flate/testdata/huffman-text-shift.dyn.expect", "src/compress/flate/testdata/huffman-text-shift.dyn.expect-noinput", "src/compress/flate/testdata/huffman-text-shift.golden", "src/compress/flate/testdata/huffman-text-shift.in", "src/compress/flate/testdata/huffman-text-shift.wb.expect", "src/compress/flate/testdata/huffman-text-shift.wb.expect-noinput", "src/compress/flate/testdata/huffman-text.dyn.expect", "src/compress/flate/testdata/huffman-text.dyn.expect-noinput", "src/compress/flate/testdata/huffman-text.golden", "src/compress/flate/testdata/huffman-text.in", "src/compress/flate/testdata/huffman-text.wb.expect", "src/compress/flate/testdata/huffman-text.wb.expect-noinput", "src/compress/flate/testdata/huffman-zero.dyn.expect", "src/compress/flate/testdata/huffman-zero.dyn.expect-noinput", "src/compress/flate/testdata/huffman-zero.golden", "src/compress/flate/testdata/huffman-zero.in", "src/compress/flate/testdata/huffman-zero.wb.expect", "src/compress/flate/testdata/huffman-zero.wb.expect-noinput", "src/compress/flate/testdata/null-long-match.dyn.expect-noinput", "src/compress/flate/testdata/null-long-match.wb.expect-noinput", "src/compress/flate/token.go", "src/compress/flate/writer_test.go", "src/compress/gzip/example_test.go", "src/compress/gzip/gunzip.go", "src/compress/gzip/gunzip_test.go", "src/compress/gzip/gzip.go", "src/compress/gzip/gzip_test.go", "src/compress/gzip/issue14937_test.go", "src/compress/gzip/testdata/issue6550.gz.base64", "src/compress/lzw/reader.go", "src/compress/lzw/reader_test.go", "src/compress/lzw/writer.go", "src/compress/lzw/writer_test.go", "src/compress/testdata/e.txt", "src/compress/testdata/gettysburg.txt", "src/compress/testdata/pi.txt", "src/compress/zlib/example_test.go", "src/compress/zlib/reader.go", "src/compress/zlib/reader_test.go", "src/compress/zlib/writer.go", "src/compress/zlib/writer_test.go"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterPrefixes(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	data.filterPrefixesOnly()
	got := data.entries().names()
	want := []string{"src/compress/bzip2/", "src/compress/bzip2/testdata/", "src/compress/flate/", "src/compress/flate/testdata/", "src/compress/gzip/", "src/compress/gzip/testdata/", "src/compress/lzw/", "src/compress/testdata/", "src/compress/zlib/"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterRecursive(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	data.filterRecursiveEntries("src/compress/bzip2/", slashSeparator)
	got := data.entries().names()
	want := []string{"src/compress/bzip2/", "src/compress/bzip2/bit_reader.go", "src/compress/bzip2/bzip2.go", "src/compress/bzip2/bzip2_test.go", "src/compress/bzip2/huffman.go", "src/compress/bzip2/move_to_front.go"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterRecursiveRoot(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	data.filterRecursiveEntries("", slashSeparator)
	got := data.entries().names()
	want := []string{}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterRecursiveRootSep(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	// This will remove anything with "bzip2/" in the path since it is separator
	data.filterRecursiveEntries("", "bzip2/")
	got := data.entries().names()
	want := []string{"src/compress/flate/", "src/compress/flate/deflate.go", "src/compress/flate/deflate_test.go", "src/compress/flate/deflatefast.go", "src/compress/flate/dict_decoder.go", "src/compress/flate/dict_decoder_test.go", "src/compress/flate/example_test.go", "src/compress/flate/flate_test.go", "src/compress/flate/huffman_bit_writer.go", "src/compress/flate/huffman_bit_writer_test.go", "src/compress/flate/huffman_code.go", "src/compress/flate/inflate.go", "src/compress/flate/inflate_test.go", "src/compress/flate/reader_test.go", "src/compress/flate/testdata/", "src/compress/flate/testdata/huffman-null-max.dyn.expect", "src/compress/flate/testdata/huffman-null-max.dyn.expect-noinput", "src/compress/flate/testdata/huffman-null-max.golden", "src/compress/flate/testdata/huffman-null-max.in", "src/compress/flate/testdata/huffman-null-max.wb.expect", "src/compress/flate/testdata/huffman-null-max.wb.expect-noinput", "src/compress/flate/testdata/huffman-pi.dyn.expect", "src/compress/flate/testdata/huffman-pi.dyn.expect-noinput", "src/compress/flate/testdata/huffman-pi.golden", "src/compress/flate/testdata/huffman-pi.in", "src/compress/flate/testdata/huffman-pi.wb.expect", "src/compress/flate/testdata/huffman-pi.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-1k.dyn.expect", "src/compress/flate/testdata/huffman-rand-1k.dyn.expect-noinput", "src/compress/flate/testdata/huffman-rand-1k.golden", "src/compress/flate/testdata/huffman-rand-1k.in", "src/compress/flate/testdata/huffman-rand-1k.wb.expect", "src/compress/flate/testdata/huffman-rand-1k.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-limit.dyn.expect", "src/compress/flate/testdata/huffman-rand-limit.dyn.expect-noinput", "src/compress/flate/testdata/huffman-rand-limit.golden", "src/compress/flate/testdata/huffman-rand-limit.in", "src/compress/flate/testdata/huffman-rand-limit.wb.expect", "src/compress/flate/testdata/huffman-rand-limit.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-max.golden", "src/compress/flate/testdata/huffman-rand-max.in", "src/compress/flate/testdata/huffman-shifts.dyn.expect", "src/compress/flate/testdata/huffman-shifts.dyn.expect-noinput", "src/compress/flate/testdata/huffman-shifts.golden", "src/compress/flate/testdata/huffman-shifts.in", "src/compress/flate/testdata/huffman-shifts.wb.expect", "src/compress/flate/testdata/huffman-shifts.wb.expect-noinput", "src/compress/flate/testdata/huffman-text-shift.dyn.expect", "src/compress/flate/testdata/huffman-text-shift.dyn.expect-noinput", "src/compress/flate/testdata/huffman-text-shift.golden", "src/compress/flate/testdata/huffman-text-shift.in", "src/compress/flate/testdata/huffman-text-shift.wb.expect", "src/compress/flate/testdata/huffman-text-shift.wb.expect-noinput", "src/compress/flate/testdata/huffman-text.dyn.expect", "src/compress/flate/testdata/huffman-text.dyn.expect-noinput", "src/compress/flate/testdata/huffman-text.golden", "src/compress/flate/testdata/huffman-text.in", "src/compress/flate/testdata/huffman-text.wb.expect", "src/compress/flate/testdata/huffman-text.wb.expect-noinput", "src/compress/flate/testdata/huffman-zero.dyn.expect", "src/compress/flate/testdata/huffman-zero.dyn.expect-noinput", "src/compress/flate/testdata/huffman-zero.golden", "src/compress/flate/testdata/huffman-zero.in", "src/compress/flate/testdata/huffman-zero.wb.expect", "src/compress/flate/testdata/huffman-zero.wb.expect-noinput", "src/compress/flate/testdata/null-long-match.dyn.expect-noinput", "src/compress/flate/testdata/null-long-match.wb.expect-noinput", "src/compress/flate/token.go", "src/compress/flate/writer_test.go", "src/compress/gzip/", "src/compress/gzip/example_test.go", "src/compress/gzip/gunzip.go", "src/compress/gzip/gunzip_test.go", "src/compress/gzip/gzip.go", "src/compress/gzip/gzip_test.go", "src/compress/gzip/issue14937_test.go", "src/compress/gzip/testdata/", "src/compress/gzip/testdata/issue6550.gz.base64", "src/compress/lzw/", "src/compress/lzw/reader.go", "src/compress/lzw/reader_test.go", "src/compress/lzw/writer.go", "src/compress/lzw/writer_test.go", "src/compress/testdata/", "src/compress/testdata/e.txt", "src/compress/testdata/gettysburg.txt", "src/compress/testdata/pi.txt", "src/compress/zlib/", "src/compress/zlib/example_test.go", "src/compress/zlib/reader.go", "src/compress/zlib/reader_test.go", "src/compress/zlib/writer.go", "src/compress/zlib/writer_test.go"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterPrefix(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	data.filterPrefix("src/compress/bzip2/")
	got := data.entries().names()
	want := []string{"src/compress/bzip2/", "src/compress/bzip2/bit_reader.go", "src/compress/bzip2/bzip2.go", "src/compress/bzip2/bzip2_test.go", "src/compress/bzip2/huffman.go", "src/compress/bzip2/move_to_front.go", "src/compress/bzip2/testdata/", "src/compress/bzip2/testdata/Isaac.Newton-Opticks.txt.bz2", "src/compress/bzip2/testdata/e.txt.bz2", "src/compress/bzip2/testdata/fail-issue5747.bz2", "src/compress/bzip2/testdata/pass-random1.bin", "src/compress/bzip2/testdata/pass-random1.bz2", "src/compress/bzip2/testdata/pass-random2.bin", "src/compress/bzip2/testdata/pass-random2.bz2", "src/compress/bzip2/testdata/pass-sawtooth.bz2", "src/compress/bzip2/testdata/random.data.bz2"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntry_isInDir(t *testing.T) {
	tests := []struct {
		testName string
		entry    string
		dir      string
		sep      string
		want     bool
	}{
		{
			testName: "basic-file",
			entry:    "src/file",
			dir:      "src/",
			sep:      slashSeparator,
			want:     true,
		},
		{
			testName: "basic-dir",
			entry:    "src/dir/",
			dir:      "src/",
			sep:      slashSeparator,
			want:     true,
		},
		{
			testName: "deeper-file",
			entry:    "src/dir/somewhere.ext",
			dir:      "src/",
			sep:      slashSeparator,
			want:     false,
		},
		{
			testName: "deeper-dir",
			entry:    "src/dir/somewhere/",
			dir:      "src/",
			sep:      slashSeparator,
			want:     false,
		},
		{
			testName: "root-dir",
			entry:    "doc/",
			dir:      "",
			sep:      slashSeparator,
			want:     true,
		},
		{
			testName: "root-file",
			entry:    "word.doc",
			dir:      "",
			sep:      slashSeparator,
			want:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			e := metaCacheEntry{
				name: tt.entry,
			}
			if got := e.isInDir(tt.dir, tt.sep); got != tt.want {
				t.Errorf("isInDir() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_metaCacheEntries_resolve(t *testing.T) {
	baseTime, err := time.Parse("2006/01/02", "2015/02/25")
	if err != nil {
		t.Fatal(err)
	}
	inputs := []xlMetaV2{
		0: {
			versions: []xlMetaV2ShallowVersion{
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					ModTime:   baseTime.Add(30 * time.Minute).UnixNano(),
					Signature: [4]byte{1, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
			},
		},
		// Mismatches Modtime+Signature and older...
		1: {
			versions: []xlMetaV2ShallowVersion{
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					ModTime:   baseTime.Add(15 * time.Minute).UnixNano(),
					Signature: [4]byte{2, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
			},
		},
		// Has another version prior to the one we want.
		2: {
			versions: []xlMetaV2ShallowVersion{
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					ModTime:   baseTime.Add(60 * time.Minute).UnixNano(),
					Signature: [4]byte{2, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					ModTime:   baseTime.Add(30 * time.Minute).UnixNano(),
					Signature: [4]byte{1, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
			},
		},
		// Has a completely different version id
		3: {
			versions: []xlMetaV2ShallowVersion{
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					ModTime:   baseTime.Add(60 * time.Minute).UnixNano(),
					Signature: [4]byte{1, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
			},
		},
		4: {
			versions: []xlMetaV2ShallowVersion{},
		},
		// Has a zero version id
		5: {
			versions: []xlMetaV2ShallowVersion{
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{},
					ModTime:   baseTime.Add(60 * time.Minute).UnixNano(),
					Signature: [4]byte{5, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
			},
		},
		// Zero version, modtime newer..
		6: {
			versions: []xlMetaV2ShallowVersion{
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{},
					ModTime:   baseTime.Add(90 * time.Minute).UnixNano(),
					Signature: [4]byte{6, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
			},
		},
		7: {
			versions: []xlMetaV2ShallowVersion{
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{},
					ModTime:   baseTime.Add(90 * time.Minute).UnixNano(),
					Signature: [4]byte{6, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					ModTime:   baseTime.Add(60 * time.Minute).UnixNano(),
					Signature: [4]byte{2, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					ModTime:   baseTime.Add(60 * time.Minute).UnixNano(),
					Signature: [4]byte{1, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},

				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					ModTime:   baseTime.Add(30 * time.Minute).UnixNano(),
					Signature: [4]byte{1, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
			},
		},
		// Delete marker.
		8: {
			versions: []xlMetaV2ShallowVersion{
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7},
					ModTime:   baseTime.Add(90 * time.Minute).UnixNano(),
					Signature: [4]byte{6, 1, 1, 1},
					Type:      DeleteType,
					Flags:     0,
				}},
			},
		},
		// Delete marker and version from 1
		9: {
			versions: []xlMetaV2ShallowVersion{
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7},
					ModTime:   baseTime.Add(90 * time.Minute).UnixNano(),
					Signature: [4]byte{6, 1, 1, 1},
					Type:      DeleteType,
					Flags:     0,
				}},
				{header: xlMetaV2VersionHeader{
					VersionID: [16]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					ModTime:   baseTime.Add(15 * time.Minute).UnixNano(),
					Signature: [4]byte{2, 1, 1, 1},
					Type:      ObjectType,
					Flags:     0,
				}},
			},
		},
	}
	inputSerialized := make([]metaCacheEntry, len(inputs))
	for i, xl := range inputs {
		xl.sortByModTime()
		var err error
		entry := metaCacheEntry{
			name: "testobject",
		}
		entry.metadata, err = xl.AppendTo(nil)
		if err != nil {
			t.Fatal(err)
		}
		inputSerialized[i] = entry
	}

	tests := []struct {
		name         string
		m            metaCacheEntries
		r            metadataResolutionParams
		wantSelected *metaCacheEntry
		wantOk       bool
	}{
		{
			name:         "consistent",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], inputSerialized[0]},
			r:            metadataResolutionParams{dirQuorum: 4, objQuorum: 4, strict: false},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "consistent-strict",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], inputSerialized[0]},
			r:            metadataResolutionParams{dirQuorum: 4, objQuorum: 4, strict: false},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "one zero, below quorum",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], metaCacheEntry{}},
			r:            metadataResolutionParams{dirQuorum: 4, objQuorum: 4, strict: false},
			wantSelected: nil,
			wantOk:       false,
		},
		{
			name:         "one zero, below quorum, strict",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], metaCacheEntry{}},
			r:            metadataResolutionParams{dirQuorum: 4, objQuorum: 4, strict: true},
			wantSelected: nil,
			wantOk:       false,
		},
		{
			name:         "one zero, at quorum",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], metaCacheEntry{}},
			r:            metadataResolutionParams{dirQuorum: 3, objQuorum: 3, strict: false},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "one zero, at quorum, strict",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], metaCacheEntry{}},
			r:            metadataResolutionParams{dirQuorum: 3, objQuorum: 3, strict: true},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "modtime, signature mismatch",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], inputSerialized[1]},
			r:            metadataResolutionParams{dirQuorum: 4, objQuorum: 4, strict: false},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "modtime,signature mismatch, strict",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], inputSerialized[1]},
			r:            metadataResolutionParams{dirQuorum: 4, objQuorum: 4, strict: true},
			wantSelected: nil,
			wantOk:       false,
		},
		{
			name:         "modtime, signature mismatch, at quorum",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], inputSerialized[1]},
			r:            metadataResolutionParams{dirQuorum: 3, objQuorum: 3, strict: false},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "modtime,signature mismatch, at quorum, strict",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], inputSerialized[1]},
			r:            metadataResolutionParams{dirQuorum: 3, objQuorum: 3, strict: true},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "additional version",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], inputSerialized[2]},
			r:            metadataResolutionParams{dirQuorum: 4, objQuorum: 4, strict: false},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			// Since we have the same version in all inputs, that is strictly ok.
			name:         "additional version, strict",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], inputSerialized[2]},
			r:            metadataResolutionParams{dirQuorum: 4, objQuorum: 4, strict: true},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			// Since we have the same version in all inputs, that is strictly ok.
			name: "additional version, quorum one",
			m:    metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], inputSerialized[2]},
			r:    metadataResolutionParams{dirQuorum: 1, objQuorum: 1, strict: true},
			// We get the both versions, since we only request quorum 1
			wantSelected: &inputSerialized[2],
			wantOk:       true,
		},
		{
			name:         "additional version, quorum two",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[0], inputSerialized[2]},
			r:            metadataResolutionParams{dirQuorum: 2, objQuorum: 2, strict: true},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "2 additional versions, quorum two",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[0], inputSerialized[2], inputSerialized[2]},
			r:            metadataResolutionParams{dirQuorum: 2, objQuorum: 2, strict: true},
			wantSelected: &inputSerialized[2],
			wantOk:       true,
		},
		{
			// inputSerialized[1] have older versions of the second in inputSerialized[2]
			name:         "modtimemismatch",
			m:            metaCacheEntries{inputSerialized[1], inputSerialized[1], inputSerialized[2], inputSerialized[2]},
			r:            metadataResolutionParams{dirQuorum: 2, objQuorum: 2, strict: false},
			wantSelected: &inputSerialized[2],
			wantOk:       true,
		},
		{
			// inputSerialized[1] have older versions of the second in inputSerialized[2]
			name:         "modtimemismatch,strict",
			m:            metaCacheEntries{inputSerialized[1], inputSerialized[1], inputSerialized[2], inputSerialized[2]},
			r:            metadataResolutionParams{dirQuorum: 2, objQuorum: 2, strict: true},
			wantSelected: &inputSerialized[2],
			wantOk:       true,
		},
		{
			// inputSerialized[1] have older versions of the second in inputSerialized[2], but
			// since it is not strict, we should get it that one (with latest modtime)
			name:         "modtimemismatch,not strict",
			m:            metaCacheEntries{inputSerialized[1], inputSerialized[1], inputSerialized[2], inputSerialized[2]},
			r:            metadataResolutionParams{dirQuorum: 4, objQuorum: 4, strict: false},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "one-q1",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[4], inputSerialized[4], inputSerialized[4]},
			r:            metadataResolutionParams{dirQuorum: 1, objQuorum: 1, strict: false},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "one-q1-strict",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[4], inputSerialized[4], inputSerialized[4]},
			r:            metadataResolutionParams{dirQuorum: 1, objQuorum: 1, strict: true},
			wantSelected: &inputSerialized[0],
			wantOk:       true,
		},
		{
			name:         "one-q2",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[4], inputSerialized[4], inputSerialized[4]},
			r:            metadataResolutionParams{dirQuorum: 2, objQuorum: 2, strict: false},
			wantSelected: nil,
			wantOk:       false,
		},
		{
			name:         "one-q2-strict",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[4], inputSerialized[4], inputSerialized[4]},
			r:            metadataResolutionParams{dirQuorum: 2, objQuorum: 2, strict: true},
			wantSelected: nil,
			wantOk:       false,
		},
		{
			name:         "two-diff-q2",
			m:            metaCacheEntries{inputSerialized[0], inputSerialized[3], inputSerialized[4], inputSerialized[4]},
			r:            metadataResolutionParams{dirQuorum: 2, objQuorum: 2, strict: false},
			wantSelected: nil,
			wantOk:       false,
		},
		{
			name:         "zeroid",
			m:            metaCacheEntries{inputSerialized[5], inputSerialized[5], inputSerialized[6], inputSerialized[6]},
			r:            metadataResolutionParams{dirQuorum: 2, objQuorum: 2, strict: false},
			wantSelected: &inputSerialized[6],
			wantOk:       true,
		},
		{
			// When ID is zero, do not allow non-strict matches to reach quorum.
			name:         "zeroid-belowq",
			m:            metaCacheEntries{inputSerialized[5], inputSerialized[5], inputSerialized[6], inputSerialized[6]},
			r:            metadataResolutionParams{dirQuorum: 3, objQuorum: 3, strict: false},
			wantSelected: nil,
			wantOk:       false,
		},
		{
			name:         "merge4",
			m:            metaCacheEntries{inputSerialized[2], inputSerialized[3], inputSerialized[5], inputSerialized[6]},
			r:            metadataResolutionParams{dirQuorum: 1, objQuorum: 1, strict: false},
			wantSelected: &inputSerialized[7],
			wantOk:       true,
		},
		{
			name:         "deletemarker",
			m:            metaCacheEntries{inputSerialized[8], inputSerialized[4], inputSerialized[4], inputSerialized[4]},
			r:            metadataResolutionParams{dirQuorum: 1, objQuorum: 1, strict: false},
			wantSelected: &inputSerialized[8],
			wantOk:       true,
		},
		{
			name:         "deletemarker-nonq",
			m:            metaCacheEntries{inputSerialized[8], inputSerialized[8], inputSerialized[4], inputSerialized[4]},
			r:            metadataResolutionParams{dirQuorum: 3, objQuorum: 3, strict: false},
			wantSelected: nil,
			wantOk:       false,
		},
		{
			name:         "deletemarker-nonq",
			m:            metaCacheEntries{inputSerialized[8], inputSerialized[8], inputSerialized[8], inputSerialized[1]},
			r:            metadataResolutionParams{dirQuorum: 3, objQuorum: 3, strict: false},
			wantSelected: &inputSerialized[8],
			wantOk:       true,
		},
		{
			name:         "deletemarker-mixed",
			m:            metaCacheEntries{inputSerialized[8], inputSerialized[8], inputSerialized[1], inputSerialized[1]},
			r:            metadataResolutionParams{dirQuorum: 2, objQuorum: 2, strict: false},
			wantSelected: &inputSerialized[9],
			wantOk:       true,
		},
		{
			name:         "deletemarker-q3",
			m:            metaCacheEntries{inputSerialized[8], inputSerialized[9], inputSerialized[9], inputSerialized[1]},
			r:            metadataResolutionParams{dirQuorum: 3, objQuorum: 3, strict: false},
			wantSelected: &inputSerialized[9],
			wantOk:       true,
		},
		{
			name:         "deletemarker-q3-strict",
			m:            metaCacheEntries{inputSerialized[8], inputSerialized[9], inputSerialized[9], inputSerialized[1]},
			r:            metadataResolutionParams{dirQuorum: 3, objQuorum: 3, strict: true},
			wantSelected: &inputSerialized[9],
			wantOk:       true,
		},
	}

	for testID, tt := range tests {
		rng := rand.New(rand.NewSource(0))
		// Run for a number of times, shuffling the input to ensure that output is consistent.
		for i := range 10 {
			t.Run(fmt.Sprintf("test-%d-%s-run-%d", testID, tt.name, i), func(t *testing.T) {
				if i > 0 {
					rng.Shuffle(len(tt.m), func(i, j int) {
						tt.m[i], tt.m[j] = tt.m[j], tt.m[i]
					})
				}
				gotSelected, gotOk := tt.m.resolve(&tt.r)
				if gotOk != tt.wantOk {
					t.Errorf("resolve() gotOk = %v, want %v", gotOk, tt.wantOk)
				}
				if gotSelected != nil {
					gotSelected.cached = nil
					gotSelected.reusable = false
				}
				if !reflect.DeepEqual(gotSelected, tt.wantSelected) {
					wantM, _ := tt.wantSelected.xlmeta()
					gotM, _ := gotSelected.xlmeta()
					t.Errorf("resolve() gotSelected = \n%#v, want \n%#v", *gotM, *wantM)
				}
			})
		}
	}
}

func testDir(name string) metaCacheEntry {
	return metaCacheEntry{name: name}
}

func testObject(name string, metadata ...byte) metaCacheEntry {
	if len(metadata) == 0 {
		metadata = []byte{1}
	}
	return metaCacheEntry{name: name, metadata: metadata}
}

func testNames(entries metaCacheEntries) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.name)
	}
	return out
}

func requireNames(t *testing.T, got metaCacheEntries, want []string) {
	t.Helper()
	gotNames := testNames(got)
	if len(gotNames) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(gotNames, want) {
		t.Fatalf("names mismatch\n got: %v\nwant: %v", gotNames, want)
	}
}

func testEntryChan(entries ...metaCacheEntry) chan metaCacheEntry {
	ch := make(chan metaCacheEntry, len(entries))
	for _, entry := range entries {
		ch <- entry
	}
	close(ch)
	return ch
}

func collectEntryNames(ch <-chan metaCacheEntry) []string {
	var names []string
	for entry := range ch {
		names = append(names, entry.name)
	}
	return names
}

func TestMetaCacheEntryClassification(t *testing.T) {
	tests := []struct {
		name          string
		entry         metaCacheEntry
		wantDir       bool
		wantObject    bool
		wantObjectDir bool
	}{
		{
			name:    "synthetic prefix directory",
			entry:   testDir("photos" + slashSeparator),
			wantDir: true,
		},
		{
			name:       "regular object",
			entry:      testObject("photos/image.jpg"),
			wantObject: true,
		},
		{
			name:          "object whose key ends with slash",
			entry:         testObject("photos"+slashSeparator, 1),
			wantObject:    true,
			wantObjectDir: true,
		},
		{
			name:  "empty entry",
			entry: metaCacheEntry{},
		},
		{
			name:  "name without metadata and without trailing slash",
			entry: metaCacheEntry{name: "photos"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.entry.isDir(); got != tt.wantDir {
				t.Fatalf("isDir() = %v, want %v", got, tt.wantDir)
			}
			if got := tt.entry.isObject(); got != tt.wantObject {
				t.Fatalf("isObject() = %v, want %v", got, tt.wantObject)
			}
			if got := tt.entry.isObjectDir(); got != tt.wantObjectDir {
				t.Fatalf("isObjectDir() = %v, want %v", got, tt.wantObjectDir)
			}
		})
	}
}

func TestMetaCacheEntryHasPrefix(t *testing.T) {
	entry := metaCacheEntry{name: "photos/2026/image.jpg"}
	for _, prefix := range []string{"", "photos", "photos/", "photos/2026/"} {
		if !entry.hasPrefix(prefix) {
			t.Fatalf("hasPrefix(%q) = false, want true", prefix)
		}
	}
	for _, prefix := range []string{"video", "photoz", "/photos"} {
		if entry.hasPrefix(prefix) {
			t.Fatalf("hasPrefix(%q) = true, want false", prefix)
		}
	}
}

func TestMetaCacheEntryMatchesNilAndDirectories(t *testing.T) {
	var nilEntry *metaCacheEntry
	prefer, ok := nilEntry.matches(nil, true)
	if prefer != nil || !ok {
		t.Fatalf("nil vs nil: prefer=%v ok=%v, want nil,true", prefer, ok)
	}

	dir := testDir("a/")
	prefer, ok = nilEntry.matches(&dir, true)
	if prefer != &dir || ok {
		t.Fatalf("nil vs dir: prefer=%p ok=%v, want dir,false", prefer, ok)
	}

	otherDir := testDir("a/")
	prefer, ok = dir.matches(&otherDir, true)
	if prefer != &dir || !ok {
		t.Fatalf("same synthetic dirs: prefer=%p ok=%v, want receiver,true", prefer, ok)
	}

	object := testObject("a/")
	prefer, ok = dir.matches(&object, true)
	if prefer != &dir || ok {
		t.Fatalf("dir vs object with same name: prefer=%p ok=%v, want dir,false", prefer, ok)
	}

	bDir := testDir("b/")
	prefer, ok = bDir.matches(&dir, true)
	if prefer != &dir || ok {
		t.Fatalf("different names choose lexicographically first: prefer.name=%q ok=%v, want a/,false", prefer.name, ok)
	}
}

func TestMetaCacheEntryIsInDir(t *testing.T) {
	tests := []struct {
		name      string
		entryName string
		dir       string
		separator string
		want      bool
	}{
		{"root regular object", "file.txt", "", "/", true},
		{"root synthetic directory", "photos/", "", "/", true},
		{"root recursive child", "photos/image.jpg", "", "/", false},
		{"direct child object", "photos/image.jpg", "photos/", "/", true},
		{"direct child prefix", "photos/2026/", "photos/", "/", true},
		{"recursive grandchild", "photos/2026/image.jpg", "photos/", "/", false},
		{"outside directory", "videos/image.jpg", "photos/", "/", false},
		{"prefix collision does not match as direct child", "photos-old/image.jpg", "photos/", "/", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := metaCacheEntry{name: tt.entryName}
			if got := entry.isInDir(tt.dir, tt.separator); got != tt.want {
				t.Fatalf("isInDir(%q, %q) = %v, want %v", tt.dir, tt.separator, got, tt.want)
			}
		})
	}
}

func TestMetaCacheEntryDirectoryFileInfoAndXLMetaErrors(t *testing.T) {
	dir := testDir("photos/")

	fi, err := dir.fileInfo("bucket-a")
	if err != nil {
		t.Fatalf("fileInfo(dir) returned unexpected error: %v", err)
	}
	if fi.Volume != "bucket-a" || fi.Name != "photos/" || fi.Mode != uint32(os.ModeDir) {
		t.Fatalf("unexpected directory FileInfo: %#v", fi)
	}

	fiv, err := dir.fileInfoVersions("bucket-a")
	if err != nil {
		t.Fatalf("fileInfoVersions(dir) returned unexpected error: %v", err)
	}
	if fiv.Volume != "bucket-a" || fiv.Name != "photos/" || len(fiv.Versions) != 1 || fiv.Versions[0].Mode != uint32(os.ModeDir) {
		t.Fatalf("unexpected directory FileInfoVersions: %#v", fiv)
	}

	if _, err := dir.xlmeta(); !errors.Is(err, errFileNotFound) {
		t.Fatalf("xlmeta(dir) error = %v, want errFileNotFound", err)
	}

	missingObject := metaCacheEntry{name: "not-a-dir"}
	if _, err := missingObject.xlmeta(); !errors.Is(err, errFileNotFound) {
		t.Fatalf("xlmeta(empty non-dir) error = %v, want errFileNotFound", err)
	}
}

func TestMetaCacheEntriesSortIsSortedNamesFirstFoundAndClone(t *testing.T) {
	entries := metaCacheEntries{
		testObject("c.txt", 3),
		testDir("a/"),
		testObject("b.txt", 2),
	}
	if entries.isSorted() {
		t.Fatal("isSorted() = true for unsorted input, want false")
	}

	sorted := entries.sort()
	if !sorted.o.isSorted() {
		t.Fatal("sort() did not return sorted entries")
	}
	requireNames(t, sorted.entries(), []string{"a/", "b.txt", "c.txt"})
	if !reflect.DeepEqual(sorted.o.names(), []string{"a/", "b.txt", "c.txt"}) {
		t.Fatalf("names() = %v", sorted.o.names())
	}

	first, n := metaCacheEntries{{}, testDir("first/"), testObject("second.txt")}.firstFound()
	if n != 2 || first == nil || first.name != "first/" {
		t.Fatalf("firstFound() = (%v,%d), want first/,2", first, n)
	}

	metadata := []byte{1, 2, 3}
	original := metaCacheEntries{testObject("a.txt", metadata...)}
	clone := original.shallowClone()
	clone[0].name = "changed.txt"
	if original[0].name != "a.txt" {
		t.Fatalf("shallowClone changed original entry name: %q", original[0].name)
	}
	clone[0].metadata[0] = 9
	if original[0].metadata[0] != 9 {
		t.Fatalf("shallowClone unexpectedly deep-copied metadata")
	}
}

func TestMetaCacheEntriesResolveDirectories(t *testing.T) {
	tests := []struct {
		name     string
		entries  metaCacheEntries
		params   *metadataResolutionParams
		wantOK   bool
		wantName string
	}{
		{
			name:    "nil params",
			entries: metaCacheEntries{testDir("photos/")},
			params:  nil,
		},
		{
			name:   "empty entries",
			params: &metadataResolutionParams{dirQuorum: 1, objQuorum: 1},
		},
		{
			name:     "directory reaches quorum",
			entries:  metaCacheEntries{testDir("photos/"), {}, testDir("photos/")},
			params:   &metadataResolutionParams{dirQuorum: 2, objQuorum: 2},
			wantOK:   true,
			wantName: "photos/",
		},
		{
			name:    "directory does not reach quorum",
			entries: metaCacheEntries{testDir("photos/"), {}, metaCacheEntry{name: "missing"}},
			params:  &metadataResolutionParams{dirQuorum: 2, objQuorum: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selected, ok := tt.entries.resolve(tt.params)
			if ok != tt.wantOK {
				t.Fatalf("resolve() ok = %v, want %v; selected=%#v", ok, tt.wantOK, selected)
			}
			if tt.wantOK && (selected == nil || selected.name != tt.wantName) {
				t.Fatalf("resolve() selected = %#v, want name %q", selected, tt.wantName)
			}
		})
	}
}

func TestMetaCacheEntriesSortedCloneLenEntriesAndTruncate(t *testing.T) {
	var nilSorted *metaCacheEntriesSorted
	nilSorted.truncate(1)
	if nilSorted.len() != 0 {
		t.Fatalf("nil len() = %d, want 0", nilSorted.len())
	}
	if nilSorted.entries() != nil {
		t.Fatalf("nil entries() = %#v, want nil", nilSorted.entries())
	}

	metadata := []byte{1, 2}
	m := metaCacheEntriesSorted{
		o:                metaCacheEntries{testObject("a.txt", metadata...), testObject("b.txt", 2), testDir("c/")},
		listID:           "list-1",
		reuse:            false,
		lastSkippedEntry: "skipped",
	}
	clone := m.shallowClone()
	if clone.listID != m.listID || clone.reuse != m.reuse || clone.lastSkippedEntry != m.lastSkippedEntry {
		t.Fatalf("shallowClone did not preserve metadata fields: %#v", clone)
	}
	clone.o[0].name = "changed.txt"
	if m.o[0].name != "a.txt" {
		t.Fatalf("sorted shallowClone changed original entry name: %q", m.o[0].name)
	}
	clone.o[0].metadata[0] = 9
	if m.o[0].metadata[0] != 9 {
		t.Fatalf("sorted shallowClone unexpectedly deep-copied metadata")
	}

	m.truncate(2)
	requireNames(t, m.entries(), []string{"a.txt", "b.txt"})
	m.truncate(10)
	requireNames(t, m.entries(), []string{"a.txt", "b.txt"})
}

func TestMetaCacheEntriesSortedForwardToAndPast(t *testing.T) {
	base := func() metaCacheEntriesSorted {
		return metaCacheEntriesSorted{o: metaCacheEntries{
			testObject("a.txt"),
			testObject("b.txt"),
			testObject("c.txt"),
			testObject("d.txt"),
		}}
	}

	t.Run("forwardTo keeps requested name and later entries", func(t *testing.T) {
		m := base()
		m.forwardTo("b.txt")
		requireNames(t, m.entries(), []string{"b.txt", "c.txt", "d.txt"})
	})

	t.Run("forwardTo starts at first greater name", func(t *testing.T) {
		m := base()
		m.forwardTo("bb")
		requireNames(t, m.entries(), []string{"c.txt", "d.txt"})
	})

	t.Run("forwardTo empty marker leaves list unchanged", func(t *testing.T) {
		m := base()
		m.forwardTo("")
		requireNames(t, m.entries(), []string{"a.txt", "b.txt", "c.txt", "d.txt"})
	})

	t.Run("forwardPast skips requested name", func(t *testing.T) {
		m := base()
		m.forwardPast("b.txt")
		requireNames(t, m.entries(), []string{"c.txt", "d.txt"})
	})

	t.Run("forwardPast beyond last empties list", func(t *testing.T) {
		m := base()
		m.forwardPast("z")
		requireNames(t, m.entries(), nil)
	})
}

func TestMetaCacheEntriesSortedFilters(t *testing.T) {
	t.Run("filterPrefix keeps only entries with prefix", func(t *testing.T) {
		m := metaCacheEntriesSorted{o: metaCacheEntries{
			testObject("a.txt"),
			testObject("photos/1.jpg"),
			testDir("photos/album/"),
			testObject("photos2/1.jpg"),
			testObject("z.txt"),
		}}
		m.filterPrefix("photos/")
		requireNames(t, m.entries(), []string{"photos/1.jpg", "photos/album/"})
	})

	t.Run("filterPrefix empty leaves entries unchanged", func(t *testing.T) {
		m := metaCacheEntriesSorted{o: metaCacheEntries{testObject("a.txt"), testDir("b/")}}
		m.filterPrefix("")
		requireNames(t, m.entries(), []string{"a.txt", "b/"})
	})

	t.Run("filterObjectsOnly removes synthetic dirs but keeps slash-suffixed objects", func(t *testing.T) {
		m := metaCacheEntriesSorted{o: metaCacheEntries{
			testDir("a/"),
			testObject("b.txt"),
			testObject("c/"),
		}}
		m.filterObjectsOnly()
		requireNames(t, m.entries(), []string{"b.txt", "c/"})
	})

	t.Run("filterPrefixesOnly keeps only synthetic dirs", func(t *testing.T) {
		m := metaCacheEntriesSorted{o: metaCacheEntries{
			testDir("a/"),
			testObject("b.txt"),
			testObject("c/"),
		}}
		m.filterPrefixesOnly()
		requireNames(t, m.entries(), []string{"a/"})
	})

	t.Run("filterRecursiveEntries root keeps entries without separator", func(t *testing.T) {
		m := metaCacheEntriesSorted{o: metaCacheEntries{
			testObject("a.txt"),
			testDir("photos/"),
			testObject("photos/1.jpg"),
			testObject("z.txt"),
		}}
		m.filterRecursiveEntries("", "/")
		requireNames(t, m.entries(), []string{"a.txt", "z.txt"})
	})

	t.Run("filterRecursiveEntries prefix keeps only direct non-recursive children", func(t *testing.T) {
		m := metaCacheEntriesSorted{o: metaCacheEntries{
			testObject("a.txt"),
			testObject("photos/1.jpg"),
			testDir("photos/album/"),
			testObject("photos/album/2.jpg"),
			testObject("videos/1.jpg"),
		}}
		m.filterRecursiveEntries("photos/", "/")
		requireNames(t, m.entries(), []string{"photos/1.jpg"})
	})
}

func TestMetaCacheEntriesSortedMerge(t *testing.T) {
	t.Run("unlimited merge interleaves sorted inputs", func(t *testing.T) {
		m := metaCacheEntriesSorted{o: metaCacheEntries{testObject("a.txt", 1), testObject("c.txt", 3)}}
		other := metaCacheEntriesSorted{o: metaCacheEntries{testObject("b.txt", 2), testObject("d.txt", 4)}}
		m.merge(other, -1)
		requireNames(t, m.entries(), []string{"a.txt", "b.txt", "c.txt", "d.txt"})
	})

	t.Run("positive limit truncates merged output", func(t *testing.T) {
		m := metaCacheEntriesSorted{o: metaCacheEntries{testObject("a.txt", 1), testObject("c.txt", 3)}}
		other := metaCacheEntriesSorted{o: metaCacheEntries{testObject("b.txt", 2), testObject("d.txt", 4)}}
		m.merge(other, 2)
		requireNames(t, m.entries(), []string{"a.txt", "b.txt"})
	})

	t.Run("same name and same metadata deduplicates", func(t *testing.T) {
		m := metaCacheEntriesSorted{o: metaCacheEntries{testObject("a.txt", 1), testObject("b.txt", 2)}}
		other := metaCacheEntriesSorted{o: metaCacheEntries{testObject("a.txt", 1), testObject("c.txt", 3)}}
		m.merge(other, -1)
		requireNames(t, m.entries(), []string{"a.txt", "b.txt", "c.txt"})
	})

	t.Run("same name and different metadata keeps receiver entry first per method contract", func(t *testing.T) {
		m := metaCacheEntriesSorted{o: metaCacheEntries{testObject("same.txt", 1)}}
		other := metaCacheEntriesSorted{o: metaCacheEntries{testObject("same.txt", 2)}}
		m.merge(other, -1)

		if len(m.o) != 2 {
			t.Fatalf("merge() produced %d entries, want 2", len(m.o))
		}
		if !reflect.DeepEqual(m.o[0].metadata, []byte{1}) || !reflect.DeepEqual(m.o[1].metadata, []byte{2}) {
			t.Fatalf("same-name different-metadata order = %v then %v; want receiver metadata first", m.o[0].metadata, m.o[1].metadata)
		}
	})
}

func TestMergeEntryChannels(t *testing.T) {
	t.Run("no input channels closes output", func(t *testing.T) {
		out := make(chan metaCacheEntry)
		if err := mergeEntryChannels(context.Background(), nil, out, 1); err != nil {
			t.Fatalf("mergeEntryChannels() error = %v, want nil", err)
		}
		if _, ok := <-out; ok {
			t.Fatal("output channel still open, want closed")
		}
	})

	t.Run("single channel forwards input order", func(t *testing.T) {
		out := make(chan metaCacheEntry, 2)
		err := mergeEntryChannels(context.Background(), []chan metaCacheEntry{
			testEntryChan(testObject("b.txt"), testDir("a/")),
		}, out, 1)
		if err != nil {
			t.Fatalf("mergeEntryChannels() error = %v, want nil", err)
		}
		if got, want := collectEntryNames(out), []string{"b.txt", "a/"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("output names = %v, want %v", got, want)
		}
	})

	t.Run("multiple channels merge sorted unique names", func(t *testing.T) {
		out := make(chan metaCacheEntry, 4)
		err := mergeEntryChannels(context.Background(), []chan metaCacheEntry{
			testEntryChan(testObject("a.txt"), testObject("c.txt")),
			testEntryChan(testObject("b.txt"), testObject("d.txt")),
		}, out, 1)
		if err != nil {
			t.Fatalf("mergeEntryChannels() error = %v, want nil", err)
		}
		if got, want := collectEntryNames(out), []string{"a.txt", "b.txt", "c.txt", "d.txt"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("output names = %v, want %v", got, want)
		}
	})

	t.Run("object wins over synthetic directory with same clean path", func(t *testing.T) {
		out := make(chan metaCacheEntry, 1)
		err := mergeEntryChannels(context.Background(), []chan metaCacheEntry{
			testEntryChan(testDir("a/")),
			testEntryChan(testObject("a", 1)),
		}, out, 1)
		if err != nil {
			t.Fatalf("mergeEntryChannels() error = %v, want nil", err)
		}
		if got, want := collectEntryNames(out), []string{"a"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("output names = %v, want %v", got, want)
		}
	})

	t.Run("canceled context returns context error and closes output", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out := make(chan metaCacheEntry)
		err := mergeEntryChannels(ctx, []chan metaCacheEntry{make(chan metaCacheEntry)}, out, 1)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("mergeEntryChannels() error = %v, want context.Canceled", err)
		}
		if _, ok := <-out; ok {
			t.Fatal("output channel still open after cancellation, want closed")
		}
	})
}
