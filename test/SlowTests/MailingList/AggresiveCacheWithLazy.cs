﻿using System;
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

            var requests = requestExecutor.NumberOfServerRequests;

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var cachedDocLazy = session.Advanced.Lazily.LoadAsync<Doc>("doc-1");
                var cachedDoc = await cachedDocLazy.Value;
            }

            Assert.Equal(requests, requestExecutor.NumberOfServerRequests);
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

            var requests = requestExecutor.NumberOfServerRequests;

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var cachedDocLazy = session.Advanced.Lazily.LoadAsync<Doc>("doc-1");
                var cachedDoc = await cachedDocLazy.Value;
            }

            Assert.Equal(requests, requestExecutor.NumberOfServerRequests);
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
            using var store = GetDocumentStore();

            var requestExecutor = store.GetRequestExecutor();
            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1" });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var docLazy = session.Advanced.Lazily.Load<Doc>("doc-1");
                var doc = docLazy.Value;
            }

            var requests = requestExecutor.NumberOfServerRequests;

            using (var session = store.OpenSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
            {
                var cachedDocLazy = session.Advanced.Lazily.Load<Doc>("doc-1");
                var cachedDoc = cachedDocLazy.Value;
            }

            Assert.Equal(requests, requestExecutor.NumberOfServerRequests);
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
