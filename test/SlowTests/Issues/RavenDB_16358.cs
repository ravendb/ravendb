using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16358 : RavenTestBase
    {
        public RavenDB_16358(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AggressiveCacheWithTimeoutTestAsync()
        {
            using var store = GetDocumentStore();

            var requestExecutor = store.GetRequestExecutor();
            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1" });
                session.SaveChanges();
            }

            await LoadDataAsync(store);

            var requests = requestExecutor.NumberOfServerRequests;

            await LoadDataAsync(store); //got 'Not modified' from server - update httpCacheItem age to 0

            Assert.Equal(requests, requestExecutor.NumberOfServerRequests);

            await Task.Delay(500); // cache timed out

            await LoadDataAsync(store);

            // additional request expected after cache timed out
            Assert.Equal(requests + 1, requestExecutor.NumberOfServerRequests);

            await LoadDataAsync(store);

            // this should be aggressively cached without sending a request
            Assert.Equal(requests + 1, requestExecutor.NumberOfServerRequests);
        }

        private async Task LoadDataAsync(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMilliseconds(300)))
            {
                var docLazy = session.Advanced.Lazily.LoadAsync<Doc>("doc-1");
                var doc = await docLazy.Value;
            }
        }

        [Fact]
        public void AggressiveCacheWithTimeoutTest()
        {
            using var store = GetDocumentStore();

            var requestExecutor = store.GetRequestExecutor();
            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1" });
                session.SaveChanges();
            }

            LoadData(store);

            var requests = requestExecutor.NumberOfServerRequests;

            LoadData(store); //got 'Not modified' from server - update httpCacheItem age to 0

            Assert.Equal(requests, requestExecutor.NumberOfServerRequests);

            Thread.Sleep(500); // cache timed out

            LoadData(store); 

            // additional request expected after cache timed out
            Assert.Equal(requests + 1, requestExecutor.NumberOfServerRequests);

            LoadData(store);

            // this should be aggressively cached without sending a  request
            Assert.Equal(requests + 1, requestExecutor.NumberOfServerRequests);
        }

        private void LoadData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMilliseconds(300)))
            {
                var docLazy = session.Advanced.Lazily.Load<Doc>("doc-1");
                var doc = docLazy.Value;
            }
        }

        private class Doc
        {
            public string Id { get; set; }
        }
    }
}
