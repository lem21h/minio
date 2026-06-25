package cmd

import (
	"strings"
	"testing"
)

func TestListPathOptionsEncodeParseMarkerRoundTrip(t *testing.T) {
	opts := listPathOptions{
		ID:   "550e8400-e29b-41d4-a716-446655440000",
		pool: 3,
		set:  7,
	}

	encoded := opts.encodeMarker("photos/2026/img.jpg")

	wantEncoded := "photos/2026/img.jpg[minio_cache:v2,id:550e8400-e29b-41d4-a716-446655440000,p:3,s:7]"
	if encoded != wantEncoded {
		t.Fatalf("unexpected encoded marker:\nwant: %q\n got: %q", wantEncoded, encoded)
	}

	parsed := listPathOptions{
		Marker: encoded,
	}

	parsed.parseMarker()

	if parsed.Marker != "photos/2026/img.jpg" {
		t.Fatalf("expected marker without encoded suffix, got %q", parsed.Marker)
	}

	if parsed.ID != opts.ID {
		t.Fatalf("expected ID %q, got %q", opts.ID, parsed.ID)
	}

	if parsed.pool != opts.pool {
		t.Fatalf("expected pool %d, got %d", opts.pool, parsed.pool)
	}

	if parsed.set != opts.set {
		t.Fatalf("expected set %d, got %d", opts.set, parsed.set)
	}

	if parsed.Create {
		t.Fatalf("expected Create=false")
	}
}

func TestListPathOptionsEncodeMarkerWithoutIDMarksReturn(t *testing.T) {
	opts := listPathOptions{}

	encoded := opts.encodeMarker("photos/")

	want := "photos/[minio_cache:v2,return:]"
	if encoded != want {
		t.Fatalf("unexpected encoded marker:\nwant: %q\n got: %q", want, encoded)
	}

	parsed := listPathOptions{
		Marker: encoded,
	}

	parsed.parseMarker()

	if parsed.Marker != "photos/" {
		t.Fatalf("expected marker %q, got %q", "photos/", parsed.Marker)
	}

	if !parsed.Create {
		t.Fatalf("expected Create=true")
	}

	if parsed.ID == "" {
		t.Fatalf("expected generated ID to be set")
	}
}

func TestListPathOptionsEncodeMarkerInvalidIDFallsBackToReturn(t *testing.T) {
	tests := []string{
		"bad:id",
		"bad,id",
		"bad[id",
		"bad]id",
	}

	for _, id := range tests {
		t.Run(id, func(t *testing.T) {
			opts := listPathOptions{
				ID:   id,
				pool: 1,
				set:  2,
			}

			encoded := opts.encodeMarker("marker")

			want := "marker[minio_cache:v2,return:]"
			if encoded != want {
				t.Fatalf("expected invalid ID to fallback to return marker:\nwant: %q\n got: %q", want, encoded)
			}
		})
	}
}

func TestListPathOptionsParseMarkerIgnoresPlainMarker(t *testing.T) {
	opts := listPathOptions{
		Marker: "photos/2026/img.jpg",
		ID:     "existing-id",
		Create: false,
		pool:   9,
		set:    8,
	}

	opts.parseMarker()

	if opts.Marker != "photos/2026/img.jpg" {
		t.Fatalf("plain marker should not be changed, got %q", opts.Marker)
	}

	if opts.ID != "existing-id" {
		t.Fatalf("ID should not be changed, got %q", opts.ID)
	}

	if opts.pool != 9 || opts.set != 8 {
		t.Fatalf("pool/set should not be changed, got pool=%d set=%d", opts.pool, opts.set)
	}
}

func TestListPathOptionsParseMarkerIgnoresUnknownVersion(t *testing.T) {
	opts := listPathOptions{
		Marker: "photos/[minio_cache:v1,id:abc,p:1,s:2]",
		ID:     "existing-id",
		pool:   9,
		set:    8,
	}

	opts.parseMarker()

	if opts.Marker != "photos/[minio_cache:v1,id:abc,p:1,s:2]" {
		t.Fatalf("marker should not be changed for unknown version, got %q", opts.Marker)
	}

	if opts.ID != "existing-id" {
		t.Fatalf("ID should not be changed, got %q", opts.ID)
	}

	if opts.pool != 9 || opts.set != 8 {
		t.Fatalf("pool/set should not be changed, got pool=%d set=%d", opts.pool, opts.set)
	}
}

func TestListPathOptionsParseMarkerRequiresEncodedTagAsSuffix(t *testing.T) {
	opts := listPathOptions{
		Marker: "photos/[minio_cache:v2,id:abc,p:1,s:2]/extra",
		ID:     "existing-id",
		pool:   9,
		set:    8,
	}

	opts.parseMarker()

	if opts.Marker != "photos/[minio_cache:v2,id:abc,p:1,s:2]/extra" {
		t.Fatalf("marker should not be changed when encoded tag is not suffix, got %q", opts.Marker)
	}

	if opts.ID != "existing-id" {
		t.Fatalf("ID should not be changed, got %q", opts.ID)
	}

	if opts.pool != 9 || opts.set != 8 {
		t.Fatalf("pool/set should not be changed, got pool=%d set=%d", opts.pool, opts.set)
	}
}

func TestListPathOptionsParseMarkerDoesNotPanicOnMalformedMarkers(t *testing.T) {
	tests := []string{
		"",
		"photos/",
		"photos/[",
		"photos/]",
		"photos/[minio_cache:v2",
		"photos/minio_cache:v2]",
		"photos/[minio_cache:v2,id:abc,p:1,s:2",
		"photos/[garbage]",
		"photos/[minio_cache]",
		"photos/[minio_cache:v2,id]",
		"photos/[minio_cache:v2,p:not-int,s:2]",
		"photos/[minio_cache:v2,p:1,s:not-int]",
	}

	for _, marker := range tests {
		t.Run(marker, func(t *testing.T) {
			opts := listPathOptions{
				Marker: marker,
			}

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("parseMarker panicked for marker %q: %v", marker, r)
				}
			}()

			opts.parseMarker()
		})
	}
}

func TestListPathOptionsParseMarkerWithAdditionalUnknownFields(t *testing.T) {
	opts := listPathOptions{
		Marker: "marker[minio_cache:v2,id:abc,unknown:value,p:4,s:5]",
	}

	opts.parseMarker()

	if opts.Marker != "marker" {
		t.Fatalf("expected marker %q, got %q", "marker", opts.Marker)
	}

	if opts.ID != "abc" {
		t.Fatalf("expected ID %q, got %q", "abc", opts.ID)
	}

	if opts.pool != 4 {
		t.Fatalf("expected pool 4, got %d", opts.pool)
	}

	if opts.set != 5 {
		t.Fatalf("expected set 5, got %d", opts.set)
	}
}

func TestListPathOptionsParseMarkerPreservesMarkerWithEarlierBrackets(t *testing.T) {
	opts := listPathOptions{
		Marker: "photos/[folder]/image[minio_cache:v2,id:abc,p:1,s:2]",
	}

	opts.parseMarker()

	if opts.Marker != "photos/[folder]/image" {
		t.Fatalf("expected marker before encoded suffix to be preserved, got %q", opts.Marker)
	}

	if opts.ID != "abc" {
		t.Fatalf("expected ID abc, got %q", opts.ID)
	}

	if opts.pool != 1 {
		t.Fatalf("expected pool 1, got %d", opts.pool)
	}

	if opts.set != 2 {
		t.Fatalf("expected set 2, got %d", opts.set)
	}
}

func TestListPathOptionsParseReturnMarkerCreatesNewListing(t *testing.T) {
	opts := listPathOptions{
		Marker: "marker[minio_cache:v2,return:]",
	}

	opts.parseMarker()

	if opts.Marker != "marker" {
		t.Fatalf("expected marker %q, got %q", "marker", opts.Marker)
	}

	if !opts.Create {
		t.Fatalf("expected Create=true")
	}

	if opts.ID == "" {
		t.Fatalf("expected generated ID")
	}
}

func TestListPathOptionsEncodedMarkerDoesNotContainInvalidIDCharacters(t *testing.T) {
	opts := listPathOptions{
		ID: "valid-id",
	}

	encoded := opts.encodeMarker("marker")

	if strings.Contains(encoded, "return:") {
		t.Fatalf("did not expect valid ID to fallback to return marker: %q", encoded)
	}

	if !strings.Contains(encoded, "id:valid-id") {
		t.Fatalf("expected encoded marker to contain valid ID, got %q", encoded)
	}
}
