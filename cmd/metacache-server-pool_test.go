package cmd

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio/internal/bucket/lifecycle"
)

func rawListPathOptionsForErrorTest() listPathOptions {
	return listPathOptions{
		Bucket:    "testbucket",
		Prefix:    "photos/",
		Separator: slashSeparator,
		Recursive: false,
		Limit:     100,

		// Important: avoid metacache/RPC branches.
		Transient: true,
		Create:    true,
	}
}

func Test_erasureServerPools_listPath_InvalidBucketReturnsError(t *testing.T) {
	z := &erasureServerPools{}

	o := listPathOptions{
		Bucket: "",
		Prefix: "photos/",
		Limit:  100,
	}

	entries, err := z.listPath(context.Background(), &o)
	if err == nil {
		t.Fatal("expected error for invalid bucket, got nil")
	}

	if errors.Is(err, io.EOF) {
		t.Fatalf("expected validation error, got io.EOF")
	}

	if entries.len() != 0 {
		t.Fatalf("entries length = %d, want 0", entries.len())
	}
}

func Test_erasureServerPools_listPath_CanceledContextReturnsCanceled(t *testing.T) {
	z := &erasureServerPools{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	o := rawListPathOptionsForErrorTest()

	entries, err := z.listPath(ctx, &o)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("listPath error = %v, want context.Canceled", err)
	}

	if entries.len() != 0 {
		t.Fatalf("entries length = %d, want 0", entries.len())
	}
}

func Test_erasureServerPools_listPath_EarlyReturnsEOF(t *testing.T) {
	z := &erasureServerPools{}
	ctx := context.Background()

	tests := []struct {
		name string
		opts listPathOptions
	}{
		{
			name: "limit zero",
			opts: listPathOptions{
				Bucket: "testbucket",
				Prefix: "photos/",
				Limit:  0,
			},
		},
		{
			name: "marker outside prefix",
			opts: listPathOptions{
				Bucket: "testbucket",
				Prefix: "a/",
				Marker: "b/object",
				Limit:  100,
			},
		},
		{
			name: "prefix starts with slash",
			opts: listPathOptions{
				Bucket: "testbucket",
				Prefix: SlashSeparator,
				Limit:  100,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entries, err := z.listPath(ctx, &tt.opts)

			if !errors.Is(err, io.EOF) {
				t.Fatalf("listPath error = %v, want io.EOF", err)
			}

			if entries.len() != 0 {
				t.Fatalf("entries length = %d, want 0", entries.len())
			}
		})
	}
}

func Test_erasureServerPools_listPath_ListMergedDeadlineExceededIsReturned(t *testing.T) {
	z := &erasureServerPools{}

	oldListMergedFn := listMergedFn
	listMergedFn = func(
		z *erasureServerPools,
		ctx context.Context,
		o listPathOptions,
		filterCh chan<- metaCacheEntry,
	) error {
		close(filterCh)
		return context.DeadlineExceeded
	}
	t.Cleanup(func() {
		listMergedFn = oldListMergedFn
	})

	o := rawListPathOptionsForErrorTest()

	entries, err := z.listPath(context.Background(), &o)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("listPath error = %v, want context.DeadlineExceeded", err)
	}

	if entries.len() != 0 {
		t.Fatalf("entries length = %d, want 0", entries.len())
	}
}

func Test_erasureServerPools_listPath_MarkerBeforePrefixIsCleared(t *testing.T) {
	z := &erasureServerPools{}
	ctx := context.Background()

	o := listPathOptions{
		Bucket: "testbucket",
		Prefix: "photos/",
		Marker: "abc",
		Limit:  0,
	}

	_, err := z.listPath(ctx, &o)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("listPath error = %v, want io.EOF", err)
	}

	if o.Marker != "" {
		t.Fatalf("Marker = %q, want empty", o.Marker)
	}
}

func Test_erasureServerPools_listPath_RawListingNormalizesOptionsAndCallsListMerged(t *testing.T) {
	z := &erasureServerPools{}
	ctx := context.Background()

	oldListMergedFn := listMergedFn
	t.Cleanup(func() {
		listMergedFn = oldListMergedFn
	})

	gotOptions := make(chan listPathOptions, 1)

	listMergedFn = func(
		z *erasureServerPools,
		ctx context.Context,
		o listPathOptions,
		filterCh chan<- metaCacheEntry,
	) error {
		gotOptions <- o
		close(filterCh)
		return io.EOF
	}

	o := listPathOptions{
		Bucket:    "testbucket",
		Prefix:    "photos/2026/",
		Separator: slashSeparator,
		Recursive: false,
		Limit:     100,

		// Force raw listing path and avoid metacache RPC/cache lookup branches.
		Transient: true,
		Create:    true,
	}

	entries, err := z.listPath(ctx, &o)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("listPath error = %v, want io.EOF", err)
	}

	if entries.len() != 0 {
		t.Fatalf("entries length = %d, want 0", entries.len())
	}

	var got listPathOptions

	select {
	case got = <-gotOptions:
	case <-time.After(time.Second):
		t.Fatal("listMergedFn was not called")
	}

	if got.Bucket != "testbucket" {
		t.Fatalf("Bucket = %q, want %q", got.Bucket, "testbucket")
	}

	if got.Prefix != "photos/2026/" {
		t.Fatalf("Prefix = %q, want %q", got.Prefix, "photos/2026/")
	}

	if got.Separator != slashSeparator {
		t.Fatalf("Separator = %q, want %q", got.Separator, slashSeparator)
	}

	if got.Recursive {
		t.Fatal("Recursive = true, want false for slash separator non-recursive listing")
	}

	if !got.IncludeDirectories {
		t.Fatal("IncludeDirectories = false, want true for slash separator listing")
	}

	if !got.Transient {
		t.Fatal("Transient = false, want true")
	}

	if got.Create {
		t.Fatal("Create = true, want false because Transient forces Create=false")
	}

	if !got.StopDiskAtLimit {
		t.Fatal("StopDiskAtLimit = false, want true when Lifecycle is nil")
	}

	if got.BaseDir == "" {
		t.Fatal("BaseDir is empty, want it to be derived from Prefix")
	}
}

func Test_erasureServerPools_listPath_EmptySeparatorBecomesRecursiveSlashListing(t *testing.T) {
	z := &erasureServerPools{}
	ctx := context.Background()

	oldListMergedFn := listMergedFn
	t.Cleanup(func() {
		listMergedFn = oldListMergedFn
	})

	gotOptions := make(chan listPathOptions, 1)

	listMergedFn = func(
		z *erasureServerPools,
		ctx context.Context,
		o listPathOptions,
		filterCh chan<- metaCacheEntry,
	) error {
		gotOptions <- o
		close(filterCh)
		return io.EOF
	}

	o := listPathOptions{
		Bucket:    "testbucket",
		Prefix:    "photos/",
		Separator: "",
		Recursive: false,
		Limit:     100,
		Transient: true,
		Create:    true,
	}

	_, err := z.listPath(ctx, &o)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("listPath error = %v, want io.EOF", err)
	}

	var got listPathOptions

	select {
	case got = <-gotOptions:
	case <-time.After(time.Second):
		t.Fatal("listMergedFn was not called")
	}

	if got.Separator != slashSeparator {
		t.Fatalf("Separator = %q, want %q", got.Separator, slashSeparator)
	}

	if !got.Recursive {
		t.Fatal("Recursive = false, want true when original separator is empty")
	}

	if got.IncludeDirectories {
		t.Fatal("IncludeDirectories = true, want false when original separator was empty")
	}

	if got.Create {
		t.Fatal("Create = true, want false because Transient forces Create=false")
	}
}

func Test_erasureServerPools_listPath_ListMergedContextCanceledIsNotReturnedAsListErr(t *testing.T) {
	z := &erasureServerPools{}

	oldListMergedFn := listMergedFn
	listMergedFn = func(
		z *erasureServerPools,
		ctx context.Context,
		o listPathOptions,
		filterCh chan<- metaCacheEntry,
	) error {
		close(filterCh)
		return context.Canceled
	}
	t.Cleanup(func() {
		listMergedFn = oldListMergedFn
	})

	o := rawListPathOptionsForErrorTest()

	entries, err := z.listPath(context.Background(), &o)

	// listPath intentionally ignores listMerged's context.Canceled:
	//
	//   if listErr != nil && !errors.Is(listErr, context.Canceled) {
	//       return entries, listErr
	//   }
	//
	// With no entries emitted, the final result should be io.EOF.
	if !errors.Is(err, io.EOF) {
		t.Fatalf("listPath error = %v, want io.EOF", err)
	}

	if entries.len() != 0 {
		t.Fatalf("entries length = %d, want 0", entries.len())
	}
}

func Test_erasureServerPools_listPath_ListMergedErrorIsReturned(t *testing.T) {
	z := &erasureServerPools{}

	wantErr := errors.New("forced listMerged failure")

	oldListMergedFn := listMergedFn
	listMergedFn = func(
		z *erasureServerPools,
		ctx context.Context,
		o listPathOptions,
		filterCh chan<- metaCacheEntry,
	) error {
		close(filterCh)
		return wantErr
	}
	t.Cleanup(func() {
		listMergedFn = oldListMergedFn
	})

	o := listPathOptions{
		Bucket:    "testbucket",
		Prefix:    "photos/",
		Separator: slashSeparator,
		Recursive: false,
		Limit:     100,

		// Avoid metacache/RPC branches.
		Transient: true,
		Create:    true,
	}

	entries, err := z.listPath(context.Background(), &o)
	if !errors.Is(err, wantErr) {
		t.Fatalf("listPath error = %v, want %v", err, wantErr)
	}

	if entries.len() != 0 {
		t.Fatalf("entries length = %d, want 0", entries.len())
	}
}

func Test_markMetacacheListingUnusedAsyncAndClearID_UsesOriginalListID(t *testing.T) {
	const bucket = "testbucket"
	const prefix = "photos/"
	const failedID = "failed-list-id"

	gotOptions := make(chan listPathOptions, 1)

	oldMarkMetacacheListingUnused := markMetacacheListingUnused
	markMetacacheListingUnused = func(o listPathOptions) {
		gotOptions <- o
	}
	t.Cleanup(func() {
		markMetacacheListingUnused = oldMarkMetacacheListingUnused
	})

	o := &listPathOptions{
		Bucket: bucket,
		Prefix: prefix,
		ID:     failedID,
	}

	markMetacacheListingUnusedAsyncAndClearID(o)

	if o.ID != "" {
		t.Fatalf("listPathOptions.ID after clear = %q, want empty", o.ID)
	}

	select {
	case got := <-gotOptions:
		if got.ID != failedID {
			t.Fatalf("markMetacacheListingUnused got ID = %q, want %q", got.ID, failedID)
		}

		if got.Bucket != bucket {
			t.Fatalf("markMetacacheListingUnused got Bucket = %q, want %q", got.Bucket, bucket)
		}

		if got.Prefix != prefix {
			t.Fatalf("markMetacacheListingUnused got Prefix = %q, want %q", got.Prefix, prefix)
		}

	case <-time.After(time.Second):
		t.Fatal("markMetacacheListingUnused was not called")
	}
}

func Test_erasureServerPools_listMerged_DoesNotMaskMergeErrorWhenAllSetsNotFound(t *testing.T) {
	z := &erasureServerPools{
		serverPools: []*erasureSets{
			{
				sets: []*erasureObjects{
					nil,
					nil,
				},
			},
		},
	}

	wantErr := errors.New("forced merge failure")

	oldListPathOnSetFn := listPathOnSetFn
	oldMergeEntryChannelsFn := mergeEntryChannelsFn

	listPathOnSetFn = func(
		set *erasureObjects,
		ctx context.Context,
		o listPathOptions,
		results chan<- metaCacheEntry,
	) error {
		return errFileNotFound
	}

	mergeEntryChannelsFn = func(
		ctx context.Context,
		inputs []chan metaCacheEntry,
		out chan<- metaCacheEntry,
		quorum int,
	) error {
		return wantErr
	}

	t.Cleanup(func() {
		listPathOnSetFn = oldListPathOnSetFn
		mergeEntryChannelsFn = oldMergeEntryChannelsFn
	})

	results := make(chan metaCacheEntry, 10)

	err := z.listMerged(context.Background(), listPathOptions{
		Bucket: "testbucket",
		Prefix: "photos/",
		Limit:  100,
	}, results)

	if !errors.Is(err, wantErr) {
		t.Fatalf("listMerged error = %v, want merge error %v", err, wantErr)
	}
}

func Test_erasureServerPools_listMerged_AllSetsEOFReturnsEOF(t *testing.T) {
	z := &erasureServerPools{
		serverPools: []*erasureSets{
			{
				sets: []*erasureObjects{
					nil,
					nil,
				},
			},
		},
	}

	oldListPathOnSetFn := listPathOnSetFn
	oldMergeEntryChannelsFn := mergeEntryChannelsFn

	listPathOnSetFn = func(
		set *erasureObjects,
		ctx context.Context,
		o listPathOptions,
		results chan<- metaCacheEntry,
	) error {
		return io.EOF
	}

	mergeEntryChannelsFn = func(
		ctx context.Context,
		inputs []chan metaCacheEntry,
		out chan<- metaCacheEntry,
		quorum int,
	) error {
		return nil
	}

	t.Cleanup(func() {
		listPathOnSetFn = oldListPathOnSetFn
		mergeEntryChannelsFn = oldMergeEntryChannelsFn
	})

	results := make(chan metaCacheEntry, 10)

	err := z.listMerged(context.Background(), listPathOptions{
		Bucket: "testbucket",
		Prefix: "photos/",
		Limit:  100,
	}, results)

	if !errors.Is(err, io.EOF) {
		t.Fatalf("listMerged error = %v, want io.EOF", err)
	}
}

func Test_erasureServerPools_listMerged_OneSuccessfulSetReturnsNil(t *testing.T) {
	z := &erasureServerPools{
		serverPools: []*erasureSets{
			{
				sets: []*erasureObjects{
					nil,
					nil,
				},
			},
		},
	}

	var call int

	oldListPathOnSetFn := listPathOnSetFn
	oldMergeEntryChannelsFn := mergeEntryChannelsFn

	listPathOnSetFn = func(
		set *erasureObjects,
		ctx context.Context,
		o listPathOptions,
		results chan<- metaCacheEntry,
	) error {
		call++
		if call == 1 {
			return nil
		}
		return io.EOF
	}

	mergeEntryChannelsFn = func(
		ctx context.Context,
		inputs []chan metaCacheEntry,
		out chan<- metaCacheEntry,
		quorum int,
	) error {
		return nil
	}

	t.Cleanup(func() {
		listPathOnSetFn = oldListPathOnSetFn
		mergeEntryChannelsFn = oldMergeEntryChannelsFn
	})

	results := make(chan metaCacheEntry, 10)

	err := z.listMerged(context.Background(), listPathOptions{
		Bucket: "testbucket",
		Prefix: "photos/",
		Limit:  100,
	}, results)

	if err != nil {
		t.Fatalf("listMerged error = %v, want nil", err)
	}
}

func Test_erasureServerPools_listMerged_UnexpectedSetErrorIsReturned(t *testing.T) {
	z := &erasureServerPools{
		serverPools: []*erasureSets{
			{
				sets: []*erasureObjects{
					nil,
					nil,
				},
			},
		},
	}

	wantErr := errors.New("forced set failure")

	var call int

	oldListPathOnSetFn := listPathOnSetFn
	oldMergeEntryChannelsFn := mergeEntryChannelsFn

	listPathOnSetFn = func(
		set *erasureObjects,
		ctx context.Context,
		o listPathOptions,
		results chan<- metaCacheEntry,
	) error {
		call++
		if call == 1 {
			return wantErr
		}
		return io.EOF
	}

	mergeEntryChannelsFn = func(
		ctx context.Context,
		inputs []chan metaCacheEntry,
		out chan<- metaCacheEntry,
		quorum int,
	) error {
		return nil
	}

	t.Cleanup(func() {
		listPathOnSetFn = oldListPathOnSetFn
		mergeEntryChannelsFn = oldMergeEntryChannelsFn
	})

	results := make(chan metaCacheEntry, 10)

	err := z.listMerged(context.Background(), listPathOptions{
		Bucket: "testbucket",
		Prefix: "photos/",
		Limit:  100,
	}, results)

	if !errors.Is(err, wantErr) {
		t.Fatalf("listMerged error = %v, want %v", err, wantErr)
	}
}

func Test_triggerExpiryAndRepl_ListObjectsV1ExpiredObjectIsSkipped(t *testing.T) {
	const bucket = "testbucket"
	const object = "expired-object"

	oldFileInfoFn := metaCacheEntryFileInfoFn
	oldFileInfoVersionsFn := metaCacheEntryFileInfoVersionsFn
	oldEvalLifecycleFn := evalLifecycleForListObjectFn

	metaCacheEntryFileInfoFn = func(obj metaCacheEntry, bucket string) (FileInfo, error) {
		if bucket != "testbucket" {
			t.Fatalf("bucket = %q, want %q", bucket, "testbucket")
		}

		return FileInfo{
			Volume:  bucket,
			Name:    obj.name,
			Size:    1,
			ModTime: time.Now(),
		}, nil
	}

	metaCacheEntryFileInfoVersionsFn = func(obj metaCacheEntry, bucket string) (FileInfoVersions, error) {
		return FileInfoVersions{}, nil
	}

	var evalCalls int
	evalLifecycleForListObjectFn = func(ctx context.Context, o listPathOptions, objInfo ObjectInfo) lifecycle.Event {
		evalCalls++
		return lifecycle.Event{Action: lifecycle.DeleteAction}
	}

	t.Cleanup(func() {
		metaCacheEntryFileInfoFn = oldFileInfoFn
		metaCacheEntryFileInfoVersionsFn = oldFileInfoVersionsFn
		evalLifecycleForListObjectFn = oldEvalLifecycleFn
	})

	skip := triggerExpiryAndRepl(context.Background(), listPathOptions{
		Bucket:    bucket,
		V1:        true,
		Versioned: false,
		Lifecycle: &lifecycle.Lifecycle{},
	}, metaCacheEntry{
		name: object,
	})

	if !skip {
		t.Fatal("skip = false, want true for expired object in regular ListObjects V1 listing")
	}

	if evalCalls == 0 {
		t.Fatal("lifecycle was not evaluated for ListObjects V1")
	}
}

func Test_triggerExpiryAndRepl_VersionedListingDoesNotSkipExpiredObject(t *testing.T) {
	const bucket = "testbucket"
	const object = "expired-object"

	oldFileInfoFn := metaCacheEntryFileInfoFn
	oldFileInfoVersionsFn := metaCacheEntryFileInfoVersionsFn
	oldEvalLifecycleFn := evalLifecycleForListObjectFn

	metaCacheEntryFileInfoFn = func(obj metaCacheEntry, bucket string) (FileInfo, error) {
		t.Fatal("fileInfo must not be called for versioned listing skip decision")
		return FileInfo{}, nil
	}

	metaCacheEntryFileInfoVersionsFn = func(obj metaCacheEntry, bucket string) (FileInfoVersions, error) {
		return FileInfoVersions{}, nil
	}

	evalLifecycleForListObjectFn = func(ctx context.Context, o listPathOptions, objInfo ObjectInfo) lifecycle.Event {
		return lifecycle.Event{Action: lifecycle.DeleteAction}
	}

	t.Cleanup(func() {
		metaCacheEntryFileInfoFn = oldFileInfoFn
		metaCacheEntryFileInfoVersionsFn = oldFileInfoVersionsFn
		evalLifecycleForListObjectFn = oldEvalLifecycleFn
	})

	skip := triggerExpiryAndRepl(context.Background(), listPathOptions{
		Bucket:    bucket,
		Versioned: true,
		Lifecycle: &lifecycle.Lifecycle{},
	}, metaCacheEntry{
		name: object,
	})

	if skip {
		t.Fatal("skip = true, want false for versioned listing")
	}
}

func Test_triggerExpiryAndRepl_FileInfoErrorDoesNotSkip(t *testing.T) {
	const bucket = "testbucket"
	const object = "bad-object"

	oldFileInfoFn := metaCacheEntryFileInfoFn
	oldFileInfoVersionsFn := metaCacheEntryFileInfoVersionsFn
	oldEvalLifecycleFn := evalLifecycleForListObjectFn

	metaCacheEntryFileInfoFn = func(obj metaCacheEntry, bucket string) (FileInfo, error) {
		return FileInfo{}, errors.New("forced fileInfo failure")
	}

	metaCacheEntryFileInfoVersionsFn = func(obj metaCacheEntry, bucket string) (FileInfoVersions, error) {
		t.Fatal("fileInfoVersions must not be called after fileInfo failure")
		return FileInfoVersions{}, nil
	}

	evalLifecycleForListObjectFn = func(ctx context.Context, o listPathOptions, objInfo ObjectInfo) lifecycle.Event {
		t.Fatal("lifecycle must not be evaluated after fileInfo failure")
		return lifecycle.Event{}
	}

	t.Cleanup(func() {
		metaCacheEntryFileInfoFn = oldFileInfoFn
		metaCacheEntryFileInfoVersionsFn = oldFileInfoVersionsFn
		evalLifecycleForListObjectFn = oldEvalLifecycleFn
	})

	skip := triggerExpiryAndRepl(context.Background(), listPathOptions{
		Bucket:    bucket,
		Lifecycle: &lifecycle.Lifecycle{},
	}, metaCacheEntry{
		name: object,
	})

	if skip {
		t.Fatal("skip = true, want false when fileInfo fails")
	}
}

func Test_erasureServerPools_listAndSave_DiskFullMakesListingTransient(t *testing.T) {
	oldGetAvailablePoolIdxFn := listAndSaveGetAvailablePoolIdxFn
	listAndSaveGetAvailablePoolIdxFn = func(
		z *erasureServerPools,
		ctx context.Context,
		bucket string,
		object string,
		size int64,
	) int {
		return -1
	}
	t.Cleanup(func() {
		listAndSaveGetAvailablePoolIdxFn = oldGetAvailablePoolIdxFn
	})

	z := &erasureServerPools{}

	o := &listPathOptions{
		Bucket: "testbucket",
		Prefix: "photos/",
		ID:     "list-id",
		Create: true,
		Limit:  100,
	}

	entries, err := z.listAndSave(context.Background(), o)
	if !errors.Is(err, errDiskFull) {
		t.Fatalf("listAndSave error = %v, want errDiskFull", err)
	}

	if entries.len() != 0 {
		t.Fatalf("entries length = %d, want 0", entries.len())
	}

	if o.pool != 0 {
		t.Fatalf("pool = %d, want 0", o.pool)
	}

	if o.Create {
		t.Fatal("Create = true, want false")
	}

	if o.ID != "" {
		t.Fatalf("ID = %q, want empty", o.ID)
	}

	if !o.Transient {
		t.Fatal("Transient = false, want true")
	}
}

func Test_erasureServerPools_listAndSave_ListMergedErrorIsReturnedWhenNoEntries(t *testing.T) {
	wantErr := errors.New("forced listMerged failure")

	oldGetAvailablePoolIdxFn := listAndSaveGetAvailablePoolIdxFn
	oldGetHashedSetIndexFn := listAndSaveGetHashedSetIndexFn
	oldSaveMetaCacheStreamFn := listAndSaveSaveMetaCacheStreamFn
	oldListMergedFn := listAndSaveListMergedFn
	oldRestClientFromHashFn := listAndSaveRestClientFromHashFn

	listAndSaveGetAvailablePoolIdxFn = func(
		z *erasureServerPools,
		ctx context.Context,
		bucket string,
		object string,
		size int64,
	) int {
		return 0
	}

	listAndSaveGetHashedSetIndexFn = func(pool *erasureSets, id string) int {
		return 0
	}

	listAndSaveSaveMetaCacheStreamFn = func(
		saver *erasureObjects,
		ctx context.Context,
		meta *metaCacheRPC,
		saveCh <-chan metaCacheEntry,
	) error {
		for range saveCh {
		}
		return nil
	}

	listAndSaveListMergedFn = func(
		z *erasureServerPools,
		ctx context.Context,
		o listPathOptions,
		results chan<- metaCacheEntry,
	) error {
		close(results)
		return wantErr
	}

	listAndSaveRestClientFromHashFn = func(hash string) *peerRESTClient {
		return nil
	}

	t.Cleanup(func() {
		listAndSaveGetAvailablePoolIdxFn = oldGetAvailablePoolIdxFn
		listAndSaveGetHashedSetIndexFn = oldGetHashedSetIndexFn
		listAndSaveSaveMetaCacheStreamFn = oldSaveMetaCacheStreamFn
		listAndSaveListMergedFn = oldListMergedFn
		listAndSaveRestClientFromHashFn = oldRestClientFromHashFn
	})

	z := &erasureServerPools{
		serverPools: []*erasureSets{
			{
				sets: []*erasureObjects{
					nil,
				},
			},
		},
	}

	o := &listPathOptions{
		Bucket: "testbucket",
		Prefix: "photos/",
		ID:     "list-id",
		Create: true,
		Limit:  1,
	}

	type result struct {
		entries metaCacheEntriesSorted
		err     error
	}

	done := make(chan result, 1)

	go func() {
		entries, err := z.listAndSave(context.Background(), o)
		done <- result{
			entries: entries,
			err:     err,
		}
	}()

	var res result

	select {
	case res = <-done:
	case <-time.After(time.Second):
		t.Fatal("listAndSave did not return")
	}

	if res.err == nil {
		t.Fatal("expected listMerged error, got nil")
	}

	if !strings.Contains(res.err.Error(), wantErr.Error()) {
		t.Fatalf("listAndSave error = %v, want it to contain %q", res.err, wantErr.Error())
	}

	if res.entries.len() != 0 {
		t.Fatalf("entries length = %d, want 0", res.entries.len())
	}
}

func Test_listAndSaveFanOut_DoesNotSendToOutputAfterFunctionReturned(t *testing.T) {
	inCh := make(chan metaCacheEntry, 1)
	outCh := make(chan metaCacheEntry, 1)
	saveCh := make(chan metaCacheEntry, 1)

	funcReturned := true
	var funcReturnedMu sync.Mutex

	o := listPathOptions{
		Bucket:    "testbucket",
		Prefix:    "photos/",
		Recursive: true,
	}

	entry := metaCacheEntry{name: "photos/object-1"}

	if o.shouldSkip(context.Background(), entry) {
		t.Fatal("test entry is skipped by listPathOptions.shouldSkip; test setup is invalid")
	}

	inCh <- entry
	close(inCh)

	listAndSaveFanOut(
		context.Background(),
		context.Background(),
		o,
		inCh,
		outCh,
		saveCh,
		&funcReturned,
		&funcReturnedMu,
	)

	select {
	case got, ok := <-outCh:
		if ok {
			t.Fatalf("unexpected entry sent to outCh after function returned: %+v", got)
		}
	default:
	}

	got, ok := <-saveCh
	if !ok {
		t.Fatal("saveCh was closed before saved entry could be read")
	}

	if got.name != "photos/object-1" {
		t.Fatalf("saved entry name = %q, want %q", got.name, "photos/object-1")
	}

	if !got.reusable {
		t.Fatal("saved entry reusable = false, want true")
	}

	if _, ok := <-saveCh; ok {
		t.Fatal("saveCh is not closed")
	}
}
