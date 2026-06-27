package cmd

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestMetacacheManager() *metacacheManager {
	m := &metacacheManager{
		buckets: make(map[string]*bucketMetacache),
		trash:   make(map[string]metacache),
	}

	// Prevent getBucket from starting the real async initManager goroutine.
	m.init.Do(func() {})

	return m
}

func newTestBucketMetacache(bucket string) *bucketMetacache {
	return &bucketMetacache{
		bucket:     bucket,
		caches:     make(map[string]metacache),
		cachesRoot: make(map[string][]string),
	}
}

func TestMetacacheManagerUpdateCacheEntry_ReturnsTrashEntry(t *testing.T) {
	m := newTestMetacacheManager()

	update := metacache{
		id:     "cache-1",
		bucket: "bucket-a",
	}

	trashed := metacache{
		id:         "cache-1",
		bucket:     "bucket-a",
		lastUpdate: time.Now().Add(-time.Minute),
	}

	m.trash[update.id] = trashed

	got, err := m.updateCacheEntry(update)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if got.id != trashed.id {
		t.Fatalf("expected trash id %q, got %q", trashed.id, got.id)
	}

	if got.bucket != trashed.bucket {
		t.Fatalf("expected trash bucket %q, got %q", trashed.bucket, got.bucket)
	}

	if !got.lastUpdate.Equal(trashed.lastUpdate) {
		t.Fatalf("expected trash lastUpdate %v, got %v", trashed.lastUpdate, got.lastUpdate)
	}
}

func TestMetacacheManagerUpdateCacheEntry_TrashWinsOverBucket(t *testing.T) {
	m := newTestMetacacheManager()

	update := metacache{
		id:     "cache-1",
		bucket: "bucket-a",
	}

	trashed := metacache{
		id:         "cache-1",
		bucket:     "bucket-a",
		lastUpdate: time.Now().Add(-time.Minute),
	}

	b := newTestBucketMetacache(update.bucket)

	m.trash[update.id] = trashed
	m.buckets[update.bucket] = b

	got, err := m.updateCacheEntry(update)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if got.id != trashed.id {
		t.Fatalf("expected trash id %q, got %q", trashed.id, got.id)
	}

	b.mu.RLock()
	_, cacheWasUpdated := b.caches[update.id]
	bucketMarkedUpdated := b.updated
	b.mu.RUnlock()

	if cacheWasUpdated {
		t.Fatalf("bucket cache was updated even though trash entry should win")
	}

	if bucketMarkedUpdated {
		t.Fatalf("bucket was marked updated even though trash entry should win")
	}
}

func TestMetacacheManagerUpdateCacheEntry_DelegatesToExistingBucketEntry(t *testing.T) {
	m := newTestMetacacheManager()

	existing := metacache{
		id:         "cache-1",
		bucket:     "bucket-a",
		lastUpdate: time.Now().Add(-time.Minute),
	}

	update := metacache{
		id:         existing.id,
		bucket:     existing.bucket,
		lastUpdate: time.Now(),
	}

	b := newTestBucketMetacache(update.bucket)
	b.caches[existing.id] = existing

	m.buckets[update.bucket] = b

	got, err := m.updateCacheEntry(update)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if got.id != update.id {
		t.Fatalf("expected cache id %q, got %q", update.id, got.id)
	}

	if got.bucket != update.bucket {
		t.Fatalf("expected cache bucket %q, got %q", update.bucket, got.bucket)
	}

	b.mu.RLock()
	stored, ok := b.caches[update.id]
	bucketMarkedUpdated := b.updated
	b.mu.RUnlock()

	if !ok {
		t.Fatalf("expected cache entry %q to remain stored in bucket", update.id)
	}

	if stored.id != update.id {
		t.Fatalf("expected stored cache id %q, got %q", update.id, stored.id)
	}

	if stored.bucket != update.bucket {
		t.Fatalf("expected stored cache bucket %q, got %q", update.bucket, stored.bucket)
	}

	if !bucketMarkedUpdated {
		t.Fatalf("expected bucket to be marked updated")
	}
}

func TestMetacacheManagerUpdateCacheEntry_ReturnsVolumeNotFound(t *testing.T) {
	m := newTestMetacacheManager()

	update := metacache{
		id:     "cache-1",
		bucket: "missing-bucket",
	}

	got, err := m.updateCacheEntry(update)
	if !errors.Is(err, errVolumeNotFound) {
		t.Fatalf("expected errVolumeNotFound, got %v", err)
	}

	if got.id != "" {
		t.Fatalf("expected empty metacache id, got %q", got.id)
	}

	if got.bucket != "" {
		t.Fatalf("expected empty metacache bucket, got %q", got.bucket)
	}
}

func TestMetacacheManagerUpdateCacheEntry_ConcurrentTrashAccess(t *testing.T) {
	m := newTestMetacacheManager()

	update := metacache{
		id:     "cache-1",
		bucket: "bucket-a",
	}

	const readers = 16
	const iterations = 10_000

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < readers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			for j := 0; j < iterations; j++ {
				_, err := m.updateCacheEntry(update)
				if err != nil && !errors.Is(err, errVolumeNotFound) {
					t.Errorf("unexpected error: %v", err)
					return
				}
			}
		}()
	}

	wg.Add(1)

	go func() {
		defer wg.Done()

		<-start

		for i := 0; i < iterations; i++ {
			m.mu.Lock()

			if i%2 == 0 {
				m.trash[update.id] = metacache{
					id:         update.id,
					bucket:     update.bucket,
					lastUpdate: time.Now(),
				}
			} else {
				delete(m.trash, update.id)
			}

			m.mu.Unlock()
		}
	}()

	close(start)
	wg.Wait()
}

func TestMetacacheManagerGetBucket_ContextCanceledReturnsNil(t *testing.T) {
	m := newTestMetacacheManager()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := m.getBucket(ctx, "bucket-a")
	if got != nil {
		t.Fatalf("expected nil bucket for canceled context, got %#v", got)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.buckets) != 0 {
		t.Fatalf("expected no buckets to be created, got %d", len(m.buckets))
	}
}

func TestMetacacheManagerGetBucket_ReturnsExistingBucket(t *testing.T) {
	m := newTestMetacacheManager()

	expected := newTestBucketMetacache("bucket-a")
	m.buckets["bucket-a"] = expected

	got := m.getBucket(context.Background(), "bucket-a")
	if got == nil {
		t.Fatal("expected existing bucket, got nil")
	}

	if got != expected {
		t.Fatalf("expected existing bucket pointer %p, got %p", expected, got)
	}

	if got.bucket != "bucket-a" {
		t.Fatalf("expected bucket name %q, got %q", "bucket-a", got.bucket)
	}
}

func TestMetacacheManagerGetBucket_CreatesBucketWhenMissing(t *testing.T) {
	m := newTestMetacacheManager()

	got := m.getBucket(context.Background(), "bucket-a")
	if got == nil {
		t.Fatal("expected created bucket, got nil")
	}

	if got.bucket != "bucket-a" {
		t.Fatalf("expected bucket name %q, got %q", "bucket-a", got.bucket)
	}

	m.mu.RLock()
	stored := m.buckets["bucket-a"]
	bucketCount := len(m.buckets)
	m.mu.RUnlock()

	if stored == nil {
		t.Fatal("expected bucket to be stored in manager")
	}

	if stored != got {
		t.Fatalf("expected stored bucket pointer %p, got %p", got, stored)
	}

	if bucketCount != 1 {
		t.Fatalf("expected exactly 1 bucket, got %d", bucketCount)
	}
}

func TestMetacacheManagerGetBucket_ReplacesNilBucketEntry(t *testing.T) {
	m := newTestMetacacheManager()

	m.buckets["bucket-a"] = nil

	got := m.getBucket(context.Background(), "bucket-a")
	if got == nil {
		t.Fatal("expected nil map entry to be replaced with real bucket")
	}

	if got.bucket != "bucket-a" {
		t.Fatalf("expected bucket name %q, got %q", "bucket-a", got.bucket)
	}

	m.mu.RLock()
	stored := m.buckets["bucket-a"]
	bucketCount := len(m.buckets)
	m.mu.RUnlock()

	if stored == nil {
		t.Fatal("expected bucket to be stored after replacing nil entry")
	}

	if stored != got {
		t.Fatalf("expected stored bucket pointer %p, got %p", got, stored)
	}

	if bucketCount != 1 {
		t.Fatalf("expected exactly 1 bucket, got %d", bucketCount)
	}
}

func TestMetacacheManagerGetBucket_ConcurrentSameBucketReturnsSingleInstance(t *testing.T) {
	m := newTestMetacacheManager()

	const goroutines = 64

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]*bucketMetacache, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			<-start
			results[i] = m.getBucket(context.Background(), "bucket-a")
		}(i)
	}

	close(start)
	wg.Wait()

	first := results[0]
	if first == nil {
		t.Fatal("expected bucket, got nil")
	}

	for i, got := range results {
		if got == nil {
			t.Fatalf("result %d: expected bucket, got nil", i)
		}

		if got != first {
			t.Fatalf("result %d: expected same bucket pointer %p, got %p", i, first, got)
		}

		if got.bucket != "bucket-a" {
			t.Fatalf("result %d: expected bucket name %q, got %q", i, "bucket-a", got.bucket)
		}
	}

	m.mu.RLock()
	stored := m.buckets["bucket-a"]
	bucketCount := len(m.buckets)
	m.mu.RUnlock()

	if stored != first {
		t.Fatalf("expected stored bucket pointer %p, got %p", first, stored)
	}

	if bucketCount != 1 {
		t.Fatalf("expected exactly 1 bucket, got %d", bucketCount)
	}
}

func TestMetacacheManagerGetBucket_ConcurrentDifferentBuckets(t *testing.T) {
	m := newTestMetacacheManager()

	bucketNames := []string{
		"bucket-a",
		"bucket-b",
		"bucket-c",
		"bucket-d",
		"bucket-e",
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]*bucketMetacache, len(bucketNames))

	for i, bucket := range bucketNames {
		wg.Add(1)

		go func(i int, bucket string) {
			defer wg.Done()

			<-start
			results[i] = m.getBucket(context.Background(), bucket)
		}(i, bucket)
	}

	close(start)
	wg.Wait()

	for i, bucket := range bucketNames {
		got := results[i]
		if got == nil {
			t.Fatalf("bucket %q: expected bucket, got nil", bucket)
		}

		if got.bucket != bucket {
			t.Fatalf("bucket %q: expected bucket name %q, got %q", bucket, bucket, got.bucket)
		}
	}

	m.mu.RLock()
	bucketCount := len(m.buckets)
	m.mu.RUnlock()

	if bucketCount != len(bucketNames) {
		t.Fatalf("expected %d buckets, got %d", len(bucketNames), bucketCount)
	}
}

func TestMetacacheManagerDeleteBucketCache_MissingBucketDoesNothing(t *testing.T) {
	m := newTestMetacacheManager()

	otherBucket := newTestBucketMetacache("other-bucket")
	m.buckets["other-bucket"] = otherBucket

	existingTrash := metacache{
		id:         "trash-1",
		bucket:     "other-bucket",
		lastUpdate: time.Now(),
	}
	m.trash["trash-1"] = existingTrash

	m.deleteBucketCache("missing-bucket")

	m.mu.RLock()
	defer m.mu.RUnlock()

	if got := m.buckets["other-bucket"]; got != otherBucket {
		t.Fatalf("expected unrelated bucket to remain unchanged")
	}

	if _, ok := m.buckets["missing-bucket"]; ok {
		t.Fatalf("did not expect missing bucket to be created or stored")
	}

	if len(m.trash) != 1 {
		t.Fatalf("expected trash to remain unchanged, got len=%d", len(m.trash))
	}

	gotTrash := m.trash["trash-1"]
	if gotTrash.id != existingTrash.id {
		t.Fatalf("expected existing trash id %q, got %q", existingTrash.id, gotTrash.id)
	}
}

func TestMetacacheManagerDeleteBucketCache_NilBucketEntryIsRemoved(t *testing.T) {
	m := newTestMetacacheManager()

	m.buckets["bucket-a"] = nil

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("deleteBucketCache panicked for nil bucket entry: %v", r)
		}
	}()

	m.deleteBucketCache("bucket-a")

	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, ok := m.buckets["bucket-a"]; ok {
		t.Fatalf("expected nil bucket entry to be removed")
	}

	if len(m.trash) != 0 {
		t.Fatalf("expected trash to remain empty, got len=%d", len(m.trash))
	}
}

func TestMetacacheManagerDeleteBucketCache_MovesRunningCachesToTrash(t *testing.T) {
	m := newTestMetacacheManager()

	b := newTestBucketMetacache("bucket-a")

	cache1 := metacache{
		id:         "cache-1",
		bucket:     "bucket-a",
		lastUpdate: time.Now(),
	}

	cache2 := metacache{
		id:         "cache-2",
		bucket:     "bucket-a",
		lastUpdate: time.Now(),
	}

	b.caches[cache1.id] = cache1
	b.caches[cache2.id] = cache2

	m.buckets["bucket-a"] = b

	m.deleteBucketCache("bucket-a")

	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, ok := m.buckets["bucket-a"]; ok {
		t.Fatalf("expected bucket to be removed from manager")
	}

	if len(m.trash) != 2 {
		t.Fatalf("expected 2 trash entries, got %d", len(m.trash))
	}

	got1, ok := m.trash[cache1.id]
	if !ok {
		t.Fatalf("expected cache %q to be moved to trash", cache1.id)
	}

	if got1.id != cache1.id {
		t.Fatalf("expected trash cache id %q, got %q", cache1.id, got1.id)
	}

	if got1.bucket != cache1.bucket {
		t.Fatalf("expected trash cache bucket %q, got %q", cache1.bucket, got1.bucket)
	}

	if got1.error != "Bucket deleted" {
		t.Fatalf("expected trash cache error %q, got %q", "Bucket deleted", got1.error)
	}

	if got1.status != scanStateError {
		t.Fatalf("expected trash cache status %v, got %v", scanStateError, got1.status)
	}

	got2, ok := m.trash[cache2.id]
	if !ok {
		t.Fatalf("expected cache %q to be moved to trash", cache2.id)
	}

	if got2.id != cache2.id {
		t.Fatalf("expected trash cache id %q, got %q", cache2.id, got2.id)
	}

	if got2.bucket != cache2.bucket {
		t.Fatalf("expected trash cache bucket %q, got %q", cache2.bucket, got2.bucket)
	}

	if got2.error != "Bucket deleted" {
		t.Fatalf("expected trash cache error %q, got %q", "Bucket deleted", got2.error)
	}

	if got2.status != scanStateError {
		t.Fatalf("expected trash cache status %v, got %v", scanStateError, got2.status)
	}
}

func TestMetacacheManagerDeleteBucketCache_ConcurrentCallsSameBucket(t *testing.T) {
	m := newTestMetacacheManager()

	b := newTestBucketMetacache("bucket-a")

	const cacheCount = 100
	for i := 0; i < cacheCount; i++ {
		id := fmt.Sprintf("cache-%d", i)

		b.caches[id] = metacache{
			id:         id,
			bucket:     "bucket-a",
			lastUpdate: time.Now(),
		}
	}

	m.buckets["bucket-a"] = b

	const goroutines = 16

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start
			m.deleteBucketCache("bucket-a")
		}()
	}

	close(start)
	wg.Wait()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, ok := m.buckets["bucket-a"]; ok {
		t.Fatalf("expected bucket to be removed")
	}

	if len(m.trash) != cacheCount {
		t.Fatalf("expected %d trash entries, got %d", cacheCount, len(m.trash))
	}
}

func TestMetacacheManagerDeleteAll_EmptyManagerDoesNothing(t *testing.T) {
	m := newTestMetacacheManager()

	m.deleteAll()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.buckets) != 0 {
		t.Fatalf("expected no buckets, got %d", len(m.buckets))
	}

	if len(m.trash) != 0 {
		t.Fatalf("expected no trash entries, got %d", len(m.trash))
	}
}

func TestMetacacheManagerDeleteAll_RemovesAllBuckets(t *testing.T) {
	m := newTestMetacacheManager()

	bucketA := newTestBucketMetacache("bucket-a")
	bucketB := newTestBucketMetacache("bucket-b")

	bucketA.caches["cache-a"] = metacache{
		id:         "cache-a",
		bucket:     "bucket-a",
		lastUpdate: time.Now(),
	}

	bucketB.caches["cache-b"] = metacache{
		id:         "cache-b",
		bucket:     "bucket-b",
		lastUpdate: time.Now(),
	}

	m.buckets["bucket-a"] = bucketA
	m.buckets["bucket-b"] = bucketB

	m.deleteAll()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.buckets) != 0 {
		t.Fatalf("expected all buckets to be removed, got %d", len(m.buckets))
	}

	if _, ok := m.buckets["bucket-a"]; ok {
		t.Fatalf("expected bucket-a to be removed")
	}

	if _, ok := m.buckets["bucket-b"]; ok {
		t.Fatalf("expected bucket-b to be removed")
	}
}

func TestMetacacheManagerDeleteAll_DoesNotModifyTrash(t *testing.T) {
	m := newTestMetacacheManager()

	bucketA := newTestBucketMetacache("bucket-a")
	bucketA.caches["cache-a"] = metacache{
		id:         "cache-a",
		bucket:     "bucket-a",
		lastUpdate: time.Now(),
	}

	trashEntry := metacache{
		id:         "trash-1",
		bucket:     "bucket-x",
		lastUpdate: time.Now(),
	}

	m.buckets["bucket-a"] = bucketA
	m.trash[trashEntry.id] = trashEntry

	m.deleteAll()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.buckets) != 0 {
		t.Fatalf("expected all buckets to be removed, got %d", len(m.buckets))
	}

	if len(m.trash) != 1 {
		t.Fatalf("expected trash to remain unchanged, got len=%d", len(m.trash))
	}

	gotTrash, ok := m.trash[trashEntry.id]
	if !ok {
		t.Fatalf("expected trash entry %q to remain", trashEntry.id)
	}

	if gotTrash.id != trashEntry.id {
		t.Fatalf("expected trash id %q, got %q", trashEntry.id, gotTrash.id)
	}

	if gotTrash.bucket != trashEntry.bucket {
		t.Fatalf("expected trash bucket %q, got %q", trashEntry.bucket, gotTrash.bucket)
	}
}

func TestMetacacheManagerDeleteAll_NilBucketEntryDoesNotPanic(t *testing.T) {
	m := newTestMetacacheManager()

	m.buckets["bucket-a"] = nil
	m.buckets["bucket-b"] = newTestBucketMetacache("bucket-b")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("deleteAll panicked with nil bucket entry: %v", r)
		}
	}()

	m.deleteAll()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.buckets) != 0 {
		t.Fatalf("expected all buckets to be removed, got %d", len(m.buckets))
	}
}

func TestMetacacheManagerDeleteAll_ConcurrentCalls(t *testing.T) {
	m := newTestMetacacheManager()

	const bucketCount = 100

	for i := 0; i < bucketCount; i++ {
		bucket := fmt.Sprintf("bucket-%d", i)
		cacheID := fmt.Sprintf("cache-%d", i)

		b := newTestBucketMetacache(bucket)
		b.caches[cacheID] = metacache{
			id:         cacheID,
			bucket:     bucket,
			lastUpdate: time.Now(),
		}

		m.buckets[bucket] = b
	}

	const goroutines = 16

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start
			m.deleteAll()
		}()
	}

	close(start)
	wg.Wait()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.buckets) != 0 {
		t.Fatalf("expected all buckets to be removed, got %d", len(m.buckets))
	}
}
