using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
    public class PrefetchingBug : RavenTest
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.MaxNumberOfItemsToProcessInSingleBatch = 2;
            configuration.MaxNumberOfItemsToReduceInSingleBatch = 2;
            configuration.InitialNumberOfItemsToProcessInSingleBatch = 2;
            configuration.InitialNumberOfItemsToReduceInSingleBatch = 2;
        }

        [Fact]
        public void ShouldNotSkipAnything()
        {
            using(var store = NewDocumentStore())
            {
                using(var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Active = true
                        });
                    }
                    session.SaveChanges();
                }

                WaitForIndexing(store); // waiting until the in memory queue is drained

                using(var session = store.OpenSession())
                {
                    var users = session.Query<User>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .Where(x => x.Active)
                        .ToList();
                    Assert.Equal(10, users.Count);
                }
            }
        }

        [Fact]
        public void ShouldHandleUpdateWhenUpdatedDocsAreLoadedFromDisk()
        {
            using (var store = NewDocumentStore())
            {
                store.SystemDatabase.WorkContext.StopIndexing(); // stop indexing to be able manually manage the prefetcher
                store.SystemDatabase.WorkContext.Configuration.MaxNumberOfItemsToPreFetch = 1;

                var putResult1 = store.SystemDatabase.Documents.Put("key/1", null, new RavenJObject(), new RavenJObject(), null);
                var putResult2 = store.SystemDatabase.Documents.Put("key/2", null, new RavenJObject(), new RavenJObject(), null);
                var putResult3 = store.SystemDatabase.Documents.Put("key/2", null, new RavenJObject(), new RavenJObject(), null); // update

                var docs = store.SystemDatabase.Prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Indexer, null, string.Empty).GetDocumentsBatchFrom(Raven.Abstractions.Data.Etag.Empty);

                Assert.Equal(2, docs.Count);
                Assert.Equal(putResult1.ETag, docs[0].Etag); // the document taken from memory

                Assert.Equal(putResult3.ETag, docs[1].Etag); // the updated doc loaded from disk because we limited MaxNumberOfItemsToIndexInSingleBatch
            }
        }

        [Fact]
        public void ShouldHandleUpdateWhenPrefetchingDocsIsDisabled()
        {
            using (var store = NewDocumentStore())
            {
                store.SystemDatabase.WorkContext.Configuration.DisableDocumentPreFetching = true;
                store.SystemDatabase.WorkContext.StopIndexing(); // stop indexing to be able manually manage the prefetcher

                var putResult1 = store.SystemDatabase.Documents.Put("key/1", null, new RavenJObject(), new RavenJObject(), null);
                var putResult2 = store.SystemDatabase.Documents.Put("key/2", null, new RavenJObject(), new RavenJObject(), null);
                var putResult3 = store.SystemDatabase.Documents.Put("key/2", null, new RavenJObject(), new RavenJObject(), null); // update

                var docs = store.SystemDatabase.Prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Indexer, null, string.Empty).GetDocumentsBatchFrom(Raven.Abstractions.Data.Etag.Empty);
                Assert.Equal(2, docs.Count);
                Assert.Equal(putResult1.ETag, docs[0].Etag);
                Assert.Equal(putResult3.ETag, docs[1].Etag);
            }
        }


        [Fact]
        public void ShouldFilterOutDeletedDocsIfDeletedDocsListContainsAnyItemWithHigherEtag()
        {
            using (var store = NewDocumentStore())
            {
                store.SystemDatabase.WorkContext.StopIndexing(); // stop indexing to be able manually manage the prefetcher
                store.SystemDatabase.WorkContext.Configuration.MaxNumberOfItemsToPreFetch = 1;

                var putResult1 = store.SystemDatabase.Documents.Put("key/1", null, new RavenJObject(), new RavenJObject(), null); // will go to prefetching queue
                var putResult2 = store.SystemDatabase.Documents.Put("key/1", null, new RavenJObject(), new RavenJObject(), null); // update - will not go into prefetching queue because MaxNumberOfItemsToPreFetchForIndexing = 1;

                var deleted = store.SystemDatabase.Documents.Delete("key/1", null, null); // delete

                Assert.True(deleted);

                var prefetchingBehavior = store.SystemDatabase.Prefetcher.CreatePrefetchingBehavior(PrefetchingUser.Indexer, null, string.Empty);
                var docs = prefetchingBehavior.GetDocumentsBatchFrom(putResult1.ETag.IncrementBy(-1)); // here we can get a document
                var filteredDocs = docs.Where(prefetchingBehavior.FilterDocuments).ToList(); // but here we should filter it out because it's already deleted!!!

                Assert.Equal(0, filteredDocs.Count);
            }
        }
    }
}
