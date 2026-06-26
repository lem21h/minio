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
	"testing"
	"time"
)

func newCleanupTestBucket(bucket string) *bucketMetacache {
	return &bucketMetacache{
		bucket:     bucket,
		caches:     make(map[string]metacache),
		cachesRoot: make(map[string][]string),
	}
}

func addCleanupTestCache(b *bucketMetacache, c metacache) {
	b.caches[c.id] = c
	b.cachesRoot[c.root] = append(b.cachesRoot[c.root], c.id)
}

func cleanupTestID(i int) string {
	return fmt.Sprintf("cleanup-test-%05d", i)
}

func cleanupTestCache(bucket, id string, lastHandout, now time.Time) metacache {
	return metacache{
		id:          id,
		bucket:      bucket,
		root:        "root/",
		started:     now.Add(-time.Minute),
		ended:       now,
		lastUpdate:  now,
		lastHandout: lastHandout,
		status:      scanStateSuccess,
	}
}

func hasRootIndex(b *bucketMetacache, root, id string) bool {
	for _, got := range b.cachesRoot[root] {
		if got == id {
			return true
		}
	}
	return false
}

func Test_bucketMetacache_deleteCache_DoesNotRecreateSameIDBeforeDiskDeleteFinished(t *testing.T) {
	const bucket = "delete-cache-race-bucket"
	const id = "delete-cache-race-id"
	const root = "root/"

	b := &bucketMetacache{
		bucket: bucket,
		caches: map[string]metacache{
			id: {
				id:     id,
				bucket: bucket,
				root:   root,
			},
		},
		cachesRoot: map[string][]string{
			root: {id},
		},
	}

	deleteStarted := make(chan struct{})
	allowDeleteFinish := make(chan struct{})
	deleteFinished := make(chan struct{})
	deleteErr := make(chan error, 1)

	oldDeleteMetacacheFiles := deleteMetacacheFiles
	deleteMetacacheFiles = func(c metacache, ctx context.Context) {
		if c.id != id {
			deleteErr <- fmt.Errorf("deleted cache id = %q, want %q", c.id, id)
			return
		}

		close(deleteStarted)
		<-allowDeleteFinish
		close(deleteFinished)
	}
	defer func() {
		deleteMetacacheFiles = oldDeleteMetacacheFiles
	}()

	go b.deleteCache(id)

	select {
	case <-deleteStarted:
	case err := <-deleteErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("deleteCache did not start disk delete")
	}

	recreated := make(chan metacache, 1)

	go func() {
		recreated <- b.findCache(listPathOptions{
			Bucket: bucket,
			ID:     id,
			Create: true,
		})
	}()

	select {
	case c := <-recreated:
		close(allowDeleteFinish)

		select {
		case <-deleteFinished:
		case <-time.After(time.Second):
			t.Fatal("disk delete did not finish")
		}

		t.Fatalf("findCache recreated cache %q before old disk delete finished: %+v", id, c)

	case <-time.After(50 * time.Millisecond):
		// Expected after the fix: findCache must not be able to recreate the same
		// ID while old on-disk files are still being deleted.
	}

	close(allowDeleteFinish)

	select {
	case <-deleteFinished:
	case <-time.After(time.Second):
		t.Fatal("disk delete did not finish after being released")
	}

	select {
	case c := <-recreated:
		if c.id != id {
			t.Fatalf("recreated cache id = %q, want %q", c.id, id)
		}
	case <-time.After(time.Second):
		t.Fatal("findCache did not complete after disk delete finished")
	}
}

func Test_bucketMetacache_cleanup_OverLimitRemovesOldestCaches(t *testing.T) {
	const bucket = "cleanup-test-bucket"

	now := time.Now().Truncate(time.Second)
	b := newCleanupTestBucket(bucket)

	total := metacacheMaxEntries + 2
	excess := total - metacacheMaxEntries

	// Spread all handout times between:
	//   > metacacheMaxClientWait
	// and
	//   < 5 * metacacheMaxClientWait
	//
	// This makes every entry:
	//   1. still worthKeeping()
	//   2. old enough to be eligible for limit-based eviction
	//
	// ID 0 is the oldest, last ID is the newest.
	step := (2 * metacacheMaxClientWait) / time.Duration(total+1)
	if step <= 0 {
		step = time.Nanosecond
	}

	for i := 0; i < total; i++ {
		id := cleanupTestID(i)

		age := metacacheMaxClientWait + step*time.Duration(total-i)
		lastHandout := now.Add(-age)

		addCleanupTestCache(b, cleanupTestCache(bucket, id, lastHandout, now))
	}

	b.cleanup()

	if got, want := len(b.caches), metacacheMaxEntries; got != want {
		t.Fatalf("cache count after cleanup = %d, want %d", got, want)
	}

	for i := 0; i < excess; i++ {
		id := cleanupTestID(i)

		if _, ok := b.caches[id]; ok {
			t.Fatalf("oldest cache %q was kept, want removed", id)
		}
		if hasRootIndex(b, "root/", id) {
			t.Fatalf("oldest cache %q still present in cachesRoot index", id)
		}
	}

	for i := excess; i < total; i++ {
		id := cleanupTestID(i)

		if _, ok := b.caches[id]; !ok {
			t.Fatalf("cache %q was removed, want kept", id)
		}
		if !hasRootIndex(b, "root/", id) {
			t.Fatalf("cache %q missing from cachesRoot index", id)
		}
	}
}

func Test_bucketMetacache_cleanup_OverLimitDoesNotOnlyLookAtNewestCaches(t *testing.T) {
	const bucket = "cleanup-test-bucket"

	now := time.Now()
	b := newCleanupTestBucket(bucket)

	total := metacacheMaxEntries + 2
	excess := total - metacacheMaxEntries

	for i := 0; i < total; i++ {
		id := cleanupTestID(i)

		var lastHandout time.Time
		if i < excess {
			// These are the only entries old enough to be evicted due to the limit.
			lastHandout = now.Add(-(metacacheMaxClientWait + metacacheMaxClientWait/2 + time.Duration(i)*time.Millisecond))
		} else {
			// These entries are newer than metacacheMaxClientWait and should not be
			// selected for limit-based eviction.
			lastHandout = now.Add(-metacacheMaxClientWait / 2)
		}

		addCleanupTestCache(b, cleanupTestCache(bucket, id, lastHandout, now))
	}

	b.cleanup()

	if got, want := len(b.caches), metacacheMaxEntries; got != want {
		t.Fatalf("cache count after cleanup = %d, want %d; cleanup probably inspected newest entries instead of oldest", got, want)
	}

	for i := 0; i < excess; i++ {
		id := cleanupTestID(i)

		if _, ok := b.caches[id]; ok {
			t.Fatalf("old eligible cache %q was kept, want removed", id)
		}
	}

	for i := excess; i < total; i++ {
		id := cleanupTestID(i)

		if _, ok := b.caches[id]; !ok {
			t.Fatalf("recent cache %q was removed, want kept", id)
		}
	}
}

func Benchmark_bucketMetacache_findCache(b *testing.B) {
	bm := newBucketMetacache("", false)
	const elements = 50000
	const paths = 100
	if elements%paths != 0 {
		b.Fatal("elements must be divisible by the number of paths")
	}
	var pathNames [paths]string
	for i := range pathNames[:] {
		pathNames[i] = fmt.Sprintf("prefix/%d", i)
	}
	for i := range elements {
		bm.findCache(listPathOptions{
			ID:           mustGetUUID(),
			Bucket:       "",
			BaseDir:      pathNames[i%paths],
			Prefix:       "",
			FilterPrefix: "",
			Marker:       "",
			Limit:        0,
			AskDisks:     "strict",
			Recursive:    false,
			Separator:    slashSeparator,
			Create:       true,
		})
	}
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		bm.findCache(listPathOptions{
			ID:           mustGetUUID(),
			Bucket:       "",
			BaseDir:      pathNames[i%paths],
			Prefix:       "",
			FilterPrefix: "",
			Marker:       "",
			Limit:        0,
			AskDisks:     "strict",
			Recursive:    false,
			Separator:    slashSeparator,
			Create:       true,
		})
	}
}
