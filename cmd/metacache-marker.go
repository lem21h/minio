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
	"fmt"
	"strconv"
	"strings"
)

// markerTagVersion is the marker version.
// Should not need to be updated unless a fundamental change is made to the marker format.
const markerTagVersion = "v2"

// parseMarker will parse a marker possibly encoded with encodeMarker
func (o *listPathOptions) parseMarker() {
	s := o.Marker
	start := strings.LastIndexByte(s, '[')
	if start < 0 {
		return
	}
	end := strings.LastIndexByte(s, ']')
	if end < 0 || end < start {
		return
	}
	// Usually encoded marker should be a suffix. If encodeMarker always appends
	// the tag at the end, require this to avoid parsing unrelated brackets.
	if end != len(s)-1 {
		return
	}

	rawTag := s[start+1 : end]

	fields := strings.Split(rawTag, ",")
	if len(fields) == 0 {
		return
	}

	parsed := make(map[string]string, len(fields))

	for _, field := range fields {
		k, v, ok := strings.Cut(field, ":")
		if !ok {
			continue
		}
		parsed[k] = v
	}

	if parsed["minio_cache"] != markerTagVersion {
		return
	}

	// Only mutate Marker after validating that this is really our marker tag.
	o.Marker = s[:start]

	if !strings.Contains(s, "[minio_cache:"+markerTagVersion) {
		return
	}
	for k, v := range parsed {
		switch k {
		case "minio_cache":
			// Already validated.

		case "id":
			o.ID = v

		case "return":
			o.ID = mustGetUUID()
			o.Create = true

		case "p": // pool
			pool, err := strconv.Atoi(v)
			if err != nil || pool < 0 {
				o.ID = mustGetUUID()
				o.Create = true
				continue
			}
			o.pool = pool

		case "s": // set
			set, err := strconv.Atoi(v)
			if err != nil || set < 0 {
				o.ID = mustGetUUID()
				o.Create = true
				continue
			}
			o.set = set

		default:
			// Ignore unknown fields.
		}
	}
}

// encodeMarker will encode a uuid and return it as a marker.
// uuid cannot contain '[', ']', ':' or ','.
func (o listPathOptions) encodeMarker(marker string) string {
	if o.ID == "" {
		// Mark as returning listing.
		return fmt.Sprintf("%s[minio_cache:%s,return:]", marker, markerTagVersion)
	}
	if strings.ContainsAny(o.ID, "[]:,") {
		internalLogIf(context.Background(), fmt.Errorf("encodeMarker: uuid %q contained invalid characters", o.ID))

		// Avoid emitting a malformed marker.
		// Caller will get a marker that asks for a new cache/listing.
		return fmt.Sprintf("%s[minio_cache:%s,return:]", marker, markerTagVersion)
	}

	return fmt.Sprintf(
		"%s[minio_cache:%s,id:%s,p:%d,s:%d]",
		marker,
		markerTagVersion,
		o.ID,
		o.pool,
		o.set,
	)
}
