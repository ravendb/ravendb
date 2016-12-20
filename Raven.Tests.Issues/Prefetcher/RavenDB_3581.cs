// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3581.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues.Prefetcher
{
    public class RavenDB_3581 : PrefetcherTestBase
    {
        [Fact]
        public void NewPrefetchingBehaviorShouldHaveEmptyQueues()
        {
            var prefetcher = CreatePrefetcher();

            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize);
        }

        [Fact]
        public void GetDocumentsBatchFromShouldThrowIfTakeIsEqualOrLessToZero()
        {
            var prefetcher = CreatePrefetcher();

            Assert.Throws<ArgumentException>(() => prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 0));
            Assert.Throws<ArgumentException>(() => prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, -100));
        }

        [Fact]
        public void GetDocumentsBatchFromShouldReturnNoResultsIfStorageIsEmpty()
        {
            var prefetcher = CreatePrefetcher();

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty);
            Assert.Equal(0, documents.Count);

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty.IncrementBy(10), 1024);
            Assert.Equal(0, documents.Count);
        }

        [Fact]
        public void GetDocumentsBatchFromShouldReturnResultsOrderedByEtag()
        {
            var prefetcher = CreatePrefetcher();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 100);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 100);
            var prevEtag = Etag.Empty;
            foreach (var document in documents)
            {
                Assert.True(EtagUtil.IsGreaterThan(document.Etag, prevEtag));
                prevEtag = document.Etag;
            }
        }

        [Fact]
        public void GetDocumentsBatchFromShouldReturnUniqueResults1()
        {
            var prefetcher = CreatePrefetcher();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 100);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 100);
            var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var document in documents)
            {
                Assert.True(keys.Add(document.Key));
            }
        }

        [Fact]
        public void GetDocumentsBatchFromShouldReturnUniqueResults2()
        {
            var prefetcher = CreatePrefetcher();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 100);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);
            var document = documents.Single();

            AddDocumentResult result = null;
            prefetcher.TransactionalStorage.Batch(accessor => result = accessor.Documents.AddDocument("keys/2", null, document.DataAsJson, document.Metadata));

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(document.Etag, 200);
            Assert.Equal(99, documents.Count);

            var prevEtag = Etag.Empty;
            var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var doc in documents)
            {
                Assert.True(EtagUtil.IsGreaterThan(document.Etag, prevEtag));
                Assert.True(keys.Add(doc.Key));
            }
        }

        [Fact]
        public void GetDocumentsBatchFromShouldNotFilterOutConflictsEvenIfAnotherDocumentWithSameKeyExists()
        {
            var prefetcher = CreatePrefetcher();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 100);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);
            var document = documents.Single();
            document.Metadata.Add(Constants.RavenReplicationConflict, true);

            AddDocumentResult result = null;
            prefetcher.TransactionalStorage.Batch(accessor => result = accessor.Documents.AddDocument("keys/2", null, document.DataAsJson, document.Metadata));

            documents.Clear();
                var etag = document.Etag;
            while (true)
            {
                var tmp = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(etag, 200);
                if (tmp.Count == 0)
                    break;
                etag = tmp.Last().Etag;
                documents.AddRange(tmp);
            }

            Assert.Equal(100, documents.Count);

            var keys = documents
                .GroupBy(x => x.Key)
                .Select(x => new KeyValuePair<string, int>(x.Key, x.Count()))
                .ToList();

            Assert.True(keys.Single(x => x.Key == "keys/2").Value == 2);
            foreach (var k in keys.Where(x => x.Key != "keys/2"))
                Assert.True(k.Value == 1);
        }

        [Fact]
        public void GetDocumentsBatchFromShouldReturnAppropriateNumberOfResults()
        {
            var prefetcher = CreatePrefetcher();

            var addedDocumentKeys = AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 1000);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 2000);

            Assert.Equal(prefetcher.AutoTuner.NumberOfItemsToProcessInSingleBatch, documents.Count);
            for (var i = 0; i < prefetcher.AutoTuner.NumberOfItemsToProcessInSingleBatch; i++)
                Assert.Equal(addedDocumentKeys[i], documents[i].Key);

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 10);

            Assert.Equal(10, documents.Count);
            for (var i = 0; i < 10; i++)
                Assert.Equal(addedDocumentKeys[i], documents[i].Key);

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(new Etag(UuidType.Documents, 0, 10), 10);

            Assert.Equal(10, documents.Count);
            for (var i = 0; i < 10; i++)
                Assert.Equal(addedDocumentKeys[i + 10], documents[i].Key);
        }

        [Fact]
        public void MaybeAddFutureBatchWillFireUpAfterDownloadingSingleDocument()
        {
            var mre = new ManualResetEventSlim();
            var count = 0;

            var prefetcher = CreatePrefetcher();
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                count += i;

                if (count == 1536)
                {
                    mre.Set();
                }
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));
            Assert.Equal(1536, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void MaybeAddFutureBatchWillNotFireUpWhenPrefetchingIsDisabled()
        {
            var mre = new ManualResetEventSlim();

            var prefetcher = CreatePrefetcher();
            prefetcher.Configuration.DisableDocumentPreFetching = true;
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i => mre.Set();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.False(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void MaybeAddFutureBatchWillNotFireUpWhenIndexingIsDisabled()
        {
            var mre = new ManualResetEventSlim();

            var prefetcher = CreatePrefetcher(modifyWorkContext: context => context.StopIndexing());
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i => mre.Set();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.False(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void MaybeAddFutureBatchWillNotFireUpWhenMaxNumberOfParallelProcessingTasksIsEqualTo1()
        {
            var mre = new ManualResetEventSlim();

            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => configuration.MaxNumberOfParallelProcessingTasks = 1);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i => mre.Set();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.False(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void MaybeAddFutureBatchWillNotFireUpWhenAvailableMemoryForRaisingBatchSizeLimitIsGreaterThanAvailableMemory()
        {
            var mre = new ManualResetEventSlim();

            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => configuration.AvailableMemoryForRaisingBatchSizeLimit = MemoryStatistics.TotalPhysicalMemory);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i => mre.Set();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.False(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void MaybeAddFutureBatchWillFireUpOnceWhenPrefetchingQueueLoadedSizeExceedsLimit()
        {
            var mre1 = new ManualResetEventSlim();
            var mre2 = new ManualResetEventSlim();
            var count = 0;

            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => configuration.MaxNumberOfItemsToProcessInSingleBatch = 128);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                count += i;
                if (count == 512)
                    mre1.Set();

                if (count > 512)
                    mre2.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.True(mre1.Wait(TimeSpan.FromSeconds(3)));
            Assert.False(mre2.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(512, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire once
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void MaybeAddFutureBatchWillFireUpOnceWhenLastActualLoadedDataExceedsAvailableMemoryForRaisingBatchSizeLimit()
        {
            var mre1 = new ManualResetEventSlim();
            var mre2 = new ManualResetEventSlim();
            var count = 0;

            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => configuration.AvailableMemoryForRaisingBatchSizeLimit = 1);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                count += i;
                if (count == 512)
                    mre1.Set();

                if (count > 1536)
                    mre2.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.True(mre1.Wait(TimeSpan.FromSeconds(3)));
            Assert.False(mre2.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(1536, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire twice
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void MaybeAddFutureBatchWillNotFireUpWhenLastActualLoadedDataExceedsAvailableMemoryForRaisingBatchSizeLimit()
        {
            var mre1 = new ManualResetEventSlim();

            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => configuration.AvailableMemoryForRaisingBatchSizeLimit = 0);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                mre1.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.False(mre1.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will not have any fututre batches
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void MaybeAddFutureBatchWillFireUpOnceWhenLastActualLoadedDataExceedsMaxNumberOfItemsToProcessInSingleBatch()
        {
            var mre1 = new ManualResetEventSlim();
            var mre2 = new ManualResetEventSlim();
            var count = 0;

            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => configuration.MaxNumberOfItemsToProcessInSingleBatch = 1023);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                count += i;
                if (count == 512)
                    mre1.Set();

                if (count > 512)
                    mre2.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.True(mre1.Wait(TimeSpan.FromSeconds(3)));
            Assert.False(mre2.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(512, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire once
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void MaybeAddFutureBatchWillFireUpLimitedNumberOfTimesIfItemsAreNotDequeued()
        {
            var mre1 = new ManualResetEventSlim();
            var mre2 = new ManualResetEventSlim();
            var count = 0;

            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => configuration.AvailableMemoryForRaisingBatchSizeLimit = 1);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                count += i;

                if (count == 512)
                    mre1.Set();

                if (count > 5200)
                    mre2.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 1024 * 10);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);
            Assert.Equal(1, documents.Count);
            Assert.True(mre1.Wait(TimeSpan.FromSeconds(3)));
            Assert.False(mre2.Wait(TimeSpan.FromSeconds(5)));
            Assert.True(5200 > prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire 10 times
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void CleanupDocuments1()
        {
            var prefetcher = CreatePrefetcher();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 1024);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1

            var etag = Etag.Empty;
            prefetcher.TransactionalStorage.Batch(accessor =>
            {
                etag = accessor.Documents.DocumentByKey("keys/510").Etag;
            });

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(etag, 1);
            Assert.Equal(1, documents.Count);
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize);
        }

        [Fact]
        public void CleanupDocuments2()
        {
            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => configuration.DisableDocumentPreFetching = true);

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 1024);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1

            var etag = Etag.Empty;
            prefetcher.TransactionalStorage.Batch(accessor =>
            {
                etag = accessor.Documents.DocumentByKey("keys/310").Etag;
            });

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(etag, 1);
            Assert.Equal(1, documents.Count);
            Assert.Equal(200, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize);

            prefetcher.TransactionalStorage.Batch(accessor =>
            {
                etag = accessor.Documents.DocumentByKey("keys/510").Etag;
            });

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(etag, 1);
            Assert.Equal(1, documents.Count);
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize);
        }

        [Fact]
        public void CleanupDocuments3()
        {
            var prefetcher = CreatePrefetcher();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 2);
            var document1 = documents[0];
            var document2 = documents[1];

            Assert.True(prefetcher.PrefetchingBehavior.FilterDocuments(document1));
            Assert.True(prefetcher.PrefetchingBehavior.FilterDocuments(document2));

            prefetcher.PrefetchingBehavior.AfterDelete(document1.Key, document1.Etag);
            prefetcher.PrefetchingBehavior.AfterDelete(document2.Key, document2.Etag);

            prefetcher.PrefetchingBehavior.CleanupDocuments(document2.Etag);

            Assert.True(prefetcher.PrefetchingBehavior.FilterDocuments(document1));
            Assert.False(prefetcher.PrefetchingBehavior.FilterDocuments(document2));
        }

        [Fact]
        public void ClearQueueAndFutureBatches()
        {
            var mre = new ManualResetEventSlim();
            var count = 0;

            var prefetcher = CreatePrefetcher();
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                count += i;

                if (count == 1536)
                    mre.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));
            Assert.True(prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize >= 1536);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1

            prefetcher.PrefetchingBehavior.ClearQueueAndFutureBatches();
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize);
        }

        [Fact]
        public void AutoTunerNumberOfItemsToProcessInSingleBatchWillLimitMaximumNumberOfItemsRetrieved()
        {
            var prefetcher = CreatePrefetcher();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            prefetcher.AutoTuner.NumberOfItemsToProcessInSingleBatch = prefetcher.Configuration.InitialNumberOfItemsToProcessInSingleBatch;
            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 2048);
            Assert.Equal(prefetcher.Configuration.InitialNumberOfItemsToProcessInSingleBatch, documents.Count);

            //let the future batches complete
            Thread.Sleep(1000);
            prefetcher.AutoTuner.NumberOfItemsToProcessInSingleBatch = 768;
            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 2048);
            Assert.Equal(768, documents.Count);

            Thread.Sleep(1000);
            prefetcher.AutoTuner.NumberOfItemsToProcessInSingleBatch = 1024;
            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 2048);
            Assert.Equal(1024, documents.Count);

            Thread.Sleep(1000);
            prefetcher.AutoTuner.NumberOfItemsToProcessInSingleBatch = 1024 * 64;
            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 2048);
            Assert.Equal(2048, documents.Count);
        }

        [Fact]
        public void ShouldSkipDeleteFromIndex()
        {
            var prefetcher = CreatePrefetcher();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);
            var document = documents.First();

            Assert.False(document.SkipDeleteFromIndex);
            Assert.False(prefetcher.PrefetchingBehavior.ShouldSkipDeleteFromIndex(document));

            document.SkipDeleteFromIndex = true;
            Assert.True(prefetcher.PrefetchingBehavior.ShouldSkipDeleteFromIndex(document));

            prefetcher.PrefetchingBehavior.AfterDelete(document.Key, document.Etag);
            Assert.False(prefetcher.PrefetchingBehavior.ShouldSkipDeleteFromIndex(document));
        }

        [Fact]
        public void FilterDocumentsShouldFilterOutIfAnyEtagIsGreaterThanCurrentForGivenKey()
        {
            var prefetcher = CreatePrefetcher();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);
            var document = documents.First();

            Assert.True(prefetcher.PrefetchingBehavior.FilterDocuments(document));

            prefetcher.PrefetchingBehavior.AfterDelete(document.Key, document.Etag.IncrementBy(-1));
            Assert.True(prefetcher.PrefetchingBehavior.FilterDocuments(document));

            prefetcher.PrefetchingBehavior.AfterDelete(document.Key, document.Etag);
            Assert.False(prefetcher.PrefetchingBehavior.FilterDocuments(document));

            prefetcher.PrefetchingBehavior.AfterDelete(document.Key, document.Etag.IncrementBy(1));
            Assert.False(prefetcher.PrefetchingBehavior.FilterDocuments(document));
        }

        [Fact]
        public void DisposeShouldCleanFutureBatches()
        {
            var mre = new ManualResetEventSlim();
            var count = 0;

            var prefetcher = CreatePrefetcher();
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                count += i;

                if (count == 1536)
                    mre.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 2048);

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1);

            Assert.Equal(1, documents.Count);
            Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));
            Assert.NotEqual(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1

            prefetcher.PrefetchingBehavior.Dispose();

            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
        }

        [Fact]
        public void wont_use_prefetcher_for_different_collection()
        {
            var mre = new ManualResetEventSlim();

            var entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Keys" };
            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => 
                configuration.AvailableMemoryForRaisingBatchSizeLimit = 1, entityNames: entityNames);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                if (i == 1)
                    mre.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 512);
            prefetcher.TransactionalStorage.Batch(accessor =>
            {
                var key = "orders/" + 1;
                var data = RavenJObject.FromObject(new Order { Id = key, Company = "companies/30" });

                accessor.Documents.AddDocument(key, null, data, new RavenJObject
                {
                    {"Raven-Entity-Name", "Orders"}
                });
            });

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 512);
            Assert.Equal(512, documents.Count);
            Assert.True(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(1, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire 1 time
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 512

            var etag = documents[511].Etag;
            entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Orders" };
            var result = prefetcher.PrefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(etag, entityNames);
            Assert.False(result);
        }

        [Fact]
        public void wont_use_prefetcher_for_different_collections()
        {
            var mre = new ManualResetEventSlim();

            var entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Keys" };
            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => 
                configuration.AvailableMemoryForRaisingBatchSizeLimit = 1, entityNames: entityNames);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                if (i == 1)
                    mre.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 512);
            prefetcher.TransactionalStorage.Batch(accessor =>
            {
                var key = "orders/" + 1;
                var data = RavenJObject.FromObject(new Order { Id = key, Company = "companies/30" });

                accessor.Documents.AddDocument(key, null, data, new RavenJObject
                {
                    {"Raven-Entity-Name", "Orders"}
                });
            });

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 512);
            Assert.Equal(512, documents.Count);
            Assert.True(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(1, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire 1 time
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 512

            var etag = documents[511].Etag;
            entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Orders", "Keys" };
            var result = prefetcher.PrefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(etag, entityNames);
            Assert.False(result);
        }

        [Fact]
        public void should_load_by_collection()
        {
            var mre = new ManualResetEventSlim();

            var entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Orders" };
            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => 
                configuration.AvailableMemoryForRaisingBatchSizeLimit = 1, entityNames: entityNames);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                if (i == 1)
                    mre.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 512);
            prefetcher.TransactionalStorage.Batch(accessor =>
            {
                var key = "orders/" + 1;
                var data = RavenJObject.FromObject(new Order { Id = key, Company = "companies/30" });

                accessor.Documents.AddDocument(key, null, data, new RavenJObject
                {
                    {"Raven-Entity-Name", "Orders"}
                });
            });

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 512);
            Assert.Equal(512, documents.Count);
            Assert.True(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(1, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire 1 time
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 512
            Assert.True(documents.Sum(x => x.DataAsJson.Count) == 0); // we get back "empty data"

            var etag = documents[511].Etag;
            var result = prefetcher.PrefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(etag, entityNames);
            Assert.True(result);

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(etag, 1);
            Assert.Equal(1, documents.Count);
            Assert.Equal(2, documents[0].DataAsJson.Count);
            // we load the from the future batch
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
        }

        [Fact]
        public void should_load_an_empty_document()
        {
            var mre = new ManualResetEventSlim();

            var entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Keys" };
            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => 
                configuration.AvailableMemoryForRaisingBatchSizeLimit = 1, entityNames: entityNames);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                if (i == 1)
                    mre.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 512);
            prefetcher.TransactionalStorage.Batch(accessor =>
            {
                var key = "orders/" + 1;
                var data = RavenJObject.FromObject(new Order { Id = key, Company = "companies/30" });

                accessor.Documents.AddDocument(key, null, data, new RavenJObject
                {
                    {"Raven-Entity-Name", "Orders"}
                });
            });

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 512);
            Assert.Equal(512, documents.Count);
            Assert.True(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(1, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire 1 time
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 512

            var etag = documents[511].Etag;
            var result = prefetcher.PrefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(etag, entityNames);
            Assert.True(result);
            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(etag, 1);
            Assert.Equal(1, documents.Count);
            Assert.Equal(0, documents[0].DataAsJson.Count);
            // we load the document from disk and discard what's in the future batch
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize);
        }

        [Fact]
        public void should_load_documents_for_specific_collection()
        {
            var mre = new ManualResetEventSlim();

            var entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Keys" };
            var prefetcher = CreatePrefetcher(modifyConfiguration: configuration => configuration.AvailableMemoryForRaisingBatchSizeLimit = 1, entityNames: entityNames);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                if (i == 512)
                    mre.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 1023);
            prefetcher.TransactionalStorage.Batch(accessor =>
            {
                var key = "orders/" + 1;
                var data = RavenJObject.FromObject(new Order { Id = key, Company = "companies/30" });

                accessor.Documents.AddDocument(key, null, data, new RavenJObject
                {
                    {"Raven-Entity-Name", "Orders"}
                });
            });

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 512);

            Assert.Equal(512, documents.Count);
            Assert.True(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(512, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire 1 time
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 512

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(documents[511].Etag, 1);
            var result = prefetcher.PrefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(documents[0].Etag, entityNames);
            Assert.True(result);
            Assert.Equal(1, documents.Count);
            Assert.Equal(3, documents[0].DataAsJson.Count);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void should_load_documents_for_sub_collection()
        {
            var mre = new ManualResetEventSlim();

            var entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Keys" , "Orders" };
            var prefetcher = CreatePrefetcher(modifyConfiguration: 
                configuration => configuration.AvailableMemoryForRaisingBatchSizeLimit = 1, entityNames: entityNames);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                if (i == 512)
                    mre.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 1023);
            prefetcher.TransactionalStorage.Batch(accessor =>
            {
                var key = "orders/" + 1;
                var data = RavenJObject.FromObject(new Order { Id = key, Company = "companies/30" });

                accessor.Documents.AddDocument(key, null, data, new RavenJObject
                {
                    {"Raven-Entity-Name", "Orders"}
                });
            });

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 512);

            Assert.Equal(512, documents.Count);
            Assert.True(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(512, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire 1 time
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 512

            entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Orders" };
            var fromEtag = documents[511].Etag;
            var result = prefetcher.PrefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(fromEtag, entityNames);
            Assert.True(result);
            result = prefetcher.PrefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(fromEtag, null);
            Assert.False(result);

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(fromEtag, 1);
            Assert.Equal(1, documents.Count);
            Assert.Equal(3, documents[0].DataAsJson.Count);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        [Fact]
        public void should_load_documents_from_all_collection()
        {
            var mre = new ManualResetEventSlim();

            var prefetcher = CreatePrefetcher(modifyConfiguration: 
                configuration => configuration.AvailableMemoryForRaisingBatchSizeLimit = 1, entityNames: null);
            prefetcher.PrefetchingBehavior.FutureBatchCompleted += i =>
            {
                if (i == 512)
                    mre.Set();
            };

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 1023);
            prefetcher.TransactionalStorage.Batch(accessor =>
            {
                var key = "orders/" + 1;
                var data = RavenJObject.FromObject(new Order { Id = key, Company = "companies/30" });

                accessor.Documents.AddDocument(key, null, data, new RavenJObject
                {
                    {"Raven-Entity-Name", "Orders"}
                });
            });

            var documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 512);

            Assert.Equal(512, documents.Count);
            Assert.True(mre.Wait(TimeSpan.FromSeconds(3)));
            Assert.Equal(512, prefetcher.PrefetchingBehavior.InMemoryFutureIndexBatchesSize); // will fire 1 time
            Assert.Equal(0, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 512

            var entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Orders" };
            var fromEtag = documents[511].Etag;
            var result = prefetcher.PrefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(fromEtag, entityNames);
            Assert.False(result);
            entityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Orders", "Keys" };
            result = prefetcher.PrefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(fromEtag, entityNames);
            Assert.False(result);
            result = prefetcher.PrefetchingBehavior.CanUsePrefetcherToLoadFromUsingExistingData(fromEtag, null);
            Assert.True(result);

            documents = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(fromEtag, 1);
            Assert.Equal(1, documents.Count);
            Assert.Equal(3, documents[0].DataAsJson.Count);
            Assert.Equal(511, prefetcher.PrefetchingBehavior.InMemoryIndexingQueueSize); // we took 1
        }

        public class Order
        {
            public string Id { get; set; }

            public string Company { get; set; }
        }
    }
}
