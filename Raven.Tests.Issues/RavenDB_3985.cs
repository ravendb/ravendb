// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3985.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Tests.Issues.Prefetcher;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3985 : PrefetcherTestBase
    {
        [Fact]
        public void when_indexing_disabled_then_indexer_prefetcher_should_not_record_deleted_items()
        {
            var prefetcher = CreatePrefetcher();

            prefetcher.WorkContext.StopIndexing();

            AddDocumentsToTransactionalStorage(prefetcher.TransactionalStorage, 1);

            var document = prefetcher.PrefetchingBehavior.GetDocumentsBatchFrom(Etag.Empty, 1).First();

            // here we verify if item is present on documentsToRemove collection by using FilterDocuments and ShouldSkipDeleteFromIndex methods

            Assert.True(prefetcher.PrefetchingBehavior.FilterDocuments(document));
            document.SkipDeleteFromIndex = true; // simulate first insert
            Assert.True(prefetcher.PrefetchingBehavior.ShouldSkipDeleteFromIndex(document));

            prefetcher.PrefetchingBehavior.AfterDelete(document.Key, document.Etag);

            Assert.True(prefetcher.PrefetchingBehavior.FilterDocuments(document));
            document.SkipDeleteFromIndex = true; // simulate first insert
            Assert.True(prefetcher.PrefetchingBehavior.ShouldSkipDeleteFromIndex(document));

            prefetcher.WorkContext.StartIndexing();

            prefetcher.PrefetchingBehavior.AfterDelete(document.Key, document.Etag);

            Assert.False(prefetcher.PrefetchingBehavior.FilterDocuments(document));
            document.SkipDeleteFromIndex = true; // simulate first insert
            Assert.False(prefetcher.PrefetchingBehavior.ShouldSkipDeleteFromIndex(document));

        }
    }
}