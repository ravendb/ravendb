using System;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class AggressiveCacheWithLazy : RavenTestBase
    {
        [Fact]
        public async Task AggresiveCacheWithLazyTestAsync()
        {
            using var store = GetDocumentStore();

            var requestExecutor = store.GetRequestExecutor();
            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1" });
                session.SaveChanges();
            }

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var docLazy = session.Advanced.Lazily.LoadAsync<Doc>("doc-1");
                var doc = await docLazy.Value;
            }

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var requests = session.Advanced.NumberOfRequests;

                var cachedDocLazy = session.Advanced.Lazily.LoadAsync<Doc>("doc-1");
                var cachedDoc = await cachedDocLazy.Value;

                Assert.Equal(requests, session.Advanced.NumberOfRequests);
            }
        }

        [Fact]
        public async Task AggresiveCacheWithLazyTestAsync_MixedMode()
        {
            using var store = GetDocumentStore();

            var requestExecutor = store.GetRequestExecutor();
            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1" });
                session.SaveChanges();
            }

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var docLazy = session.Advanced.Lazily.LoadAsync<Doc>("doc-1");
                session.Advanced.Lazily.LoadAsync<Doc>("doc-2"); // not used
                var doc = await docLazy.Value;
            }

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var requests = session.Advanced.NumberOfRequests;

                var cachedDocLazy = session.Advanced.Lazily.LoadAsync<Doc>("doc-1");
                var cachedDoc = await cachedDocLazy.Value;

                Assert.Equal(requests, session.Advanced.NumberOfRequests);
            }
        }

        [Fact]
        public async Task AggresiveCacheWithLazyTestAsync_Partly()
        {
            const string loadedDocId = "doc-1";
            const string unloadedDocId = "doc-2";

            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = loadedDocId });
                session.SaveChanges();
            }

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var docLazy = session.Advanced.Lazily.LoadAsync<Doc>(loadedDocId);
                _ = await docLazy.Value;
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var docLazy = session.Advanced.Lazily.LoadAsync<Doc>(loadedDocId);
                _ = await docLazy.Value;
                Assert.Equal(0, session.Advanced.NumberOfRequests);
            }

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var cachedDocLazy = session.Advanced.Lazily.LoadAsync<Doc>(loadedDocId);
                session.Advanced.Lazily.LoadAsync<Doc>(unloadedDocId); // not used
                _ = await cachedDocLazy.Value;

                // should force a call here
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }

        [Fact]
        public void AggresiveCacheWithLazyTest()
        {
            const string docId = "doc-1";

            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = docId });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var docLazy = session.Advanced.Lazily.Load<Doc>(docId);
                _ = docLazy.Value;
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }

            using (var session = store.OpenSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var cachedDocLazy = session.Advanced.Lazily.Load<Doc>(docId);
                _ = cachedDocLazy.Value;
                Assert.Equal(0, session.Advanced.NumberOfRequests);
            }
        }

        private class Doc
        {
            public string Id { get; set; }
        }

        public AggressiveCacheWithLazy(ITestOutputHelper output) : base(output)
        {
        }
    }
}
