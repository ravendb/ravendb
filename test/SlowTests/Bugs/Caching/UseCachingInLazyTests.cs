using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Caching
{
    public class UseCachingInLazyTests : RavenTestBase
    {
        public UseCachingInLazyTests(ITestOutputHelper output) : base(output)
        {
        }

        public class AsyncTestCaseHolder
        {
            public Func<IAsyncDocumentSession, string, Task> LoadFuncAsync;
        }


        private static async Task LoadFuncAsync(IAsyncDocumentSession s, string id) => _ = await s.LoadAsync<Doc>(id);
        private static async Task LazilyLoadFuncAsync(IAsyncDocumentSession s, string id) => _ = await s.Advanced.Lazily.LoadAsync<Doc>(id).Value;

        public static IEnumerable<object[]> AsyncTestCases
        {
            get  
            {
                return new[]
                {
                    new object[] {nameof(LoadFuncAsync), new AsyncTestCaseHolder {LoadFuncAsync = LoadFuncAsync}},
                    new object[] {nameof(LazilyLoadFuncAsync), new AsyncTestCaseHolder {LoadFuncAsync = LazilyLoadFuncAsync}}
                };
            }
        }

        [Theory]
        [MemberData(nameof(AsyncTestCases))]
        public async Task LazilyLoadAsync_WhenDoNotTrackChanges_ShouldNotCreateExtraRequest(string xunitId, AsyncTestCaseHolder testCaseHolder)
        {
            using var store = GetDocumentStore();

            var numberOfRequest = await AggressiveCacheOnLazilyLoadAsyncTest(store, testCaseHolder, AggressiveCacheMode.DoNotTrackChanges);

            Assert.Equal(0, numberOfRequest);
        }

        [Fact]
        public async Task LazilyLoadAsync_WhenTrackChangesAndChange_ShouldCreateExtraRequest()
        {
            using var store = GetDocumentStore();

            var numberOfRequest = await AggressiveCacheOnLazilyLoadAsyncTest(store, LazilyLoadFuncAsync, AggressiveCacheMode.TrackChanges);

            Assert.Equal(1, numberOfRequest);
        }

        [Fact]
        public async Task LazilyLoadAsync_WhenTrackChangesAndDoesntChange_ShouldNotCreateExtraRequest()
        {
            using var store = GetDocumentStore();

            var numberOfRequest = await AggressiveCacheOnLazilyLoadAsyncTest(store, LazilyLoadFuncAsync, AggressiveCacheMode.TrackChanges, false);

            Assert.Equal(0, numberOfRequest);
        }

        private async Task<long> AggressiveCacheOnLazilyLoadAsyncTest(
            IDocumentStore store, 
            AsyncTestCaseHolder testCaseHolder,
            AggressiveCacheMode aggressiveCacheMode,
            bool createVersion2 = true)
        {
            return await AggressiveCacheOnLazilyLoadAsyncTest(store, testCaseHolder.LoadFuncAsync, aggressiveCacheMode, createVersion2);
        }

        private static async Task<long> AggressiveCacheOnLazilyLoadAsyncTest(
            IDocumentStore store,
            Func<IAsyncDocumentSession, string, Task> loadFuncAsync,
            AggressiveCacheMode aggressiveCacheMode, 
            bool createVersion2 = true)
        {
            const string docId = "doc-1";

            var requestExecutor = store.GetRequestExecutor();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Doc(), docId);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5), AggressiveCacheMode.TrackChanges))
            {
                await loadFuncAsync(session, docId);
            }

            if (createVersion2)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Doc {Version = "2"}, docId);
                    await session.SaveChangesAsync();
                }
            }

            var requests = requestExecutor.NumberOfServerRequests;

            using (var session = store.OpenAsyncSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5), aggressiveCacheMode))
            {
                var loadAsync = session.Advanced.Lazily.LoadAsync<Doc>(docId);
                _ = await loadAsync.Value;
                return session.Advanced.NumberOfRequests;
            }
        }


        private static void LoadFunc(IDocumentSession s, string id) => _ = s.Load<Doc>(id);
        private static void LazilyLoadFunc(IDocumentSession s, string id) => _ = s.Advanced.Lazily.Load<Doc>(id).Value;

        public class TestCaseHolder
        {
            public Action<IDocumentSession, string> LoadFunc;
        }

        public static IEnumerable<object[]> TestCases
        {
            get
            {
                yield return new object[] { nameof(LoadFunc), new TestCaseHolder { LoadFunc = LoadFunc } };
                yield return new object[] { nameof(LazilyLoadFunc), new TestCaseHolder { LoadFunc = LazilyLoadFunc } };
            }
        }

        [Theory]
        [MemberData(nameof(TestCases))]
        public async Task LazilyLoad_WhenDoNotTrackChanges_ShouldNotCreateExtraRequest(string caseName, TestCaseHolder testCaseHolder)
        {
            using var store = GetDocumentStore();

            var numberOfRequest = await AggressiveCacheOnLazilyLoadTest(store, testCaseHolder, AggressiveCacheMode.DoNotTrackChanges);
            
            Assert.Equal(0, numberOfRequest);
        }

        [Fact]
        public async Task LazilyLoad_WhenTrackChangesAndChange_ShouldCreateExtraRequest()
        {
            using var store = GetDocumentStore();

            var numberOfRequest = await AggressiveCacheOnLazilyLoadTest(store, LazilyLoadFunc, AggressiveCacheMode.TrackChanges);

            Assert.Equal(1, numberOfRequest);
        }


        [Fact]
        public async Task LazilyLoad_WhenTrackChangesAndDoesntChange_ShouldNotCreateExtraRequest()
        {
            using var store = GetDocumentStore();

            var numberOfRequest = await AggressiveCacheOnLazilyLoadTest(store, LazilyLoadFunc, AggressiveCacheMode.TrackChanges, false);

            Assert.Equal(0, numberOfRequest);
        }

        private async Task<long> AggressiveCacheOnLazilyLoadTest(
            IDocumentStore store, 
            TestCaseHolder testCaseHolder,
            AggressiveCacheMode doNotTrackChanges, 
            bool createVersion2 = true)
        {
            return await AggressiveCacheOnLazilyLoadTest(store, testCaseHolder.LoadFunc, doNotTrackChanges, createVersion2);
        }

        private static async Task<long> AggressiveCacheOnLazilyLoadTest(
            IDocumentStore store, 
            Action<IDocumentSession, string> loadFunc, 
            AggressiveCacheMode doNotTrackChanges, 
            bool createVersion2 = true)
        {
            const string docId = "doc-1";

            var requestExecutor = store.GetRequestExecutor();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Doc(), docId);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5), AggressiveCacheMode.TrackChanges))
            {
                loadFunc(session, docId);
            }

            if (createVersion2)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Doc {Version = "2"}, docId);
                    await session.SaveChangesAsync();
                }
            }

            using (var session = store.OpenSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5), doNotTrackChanges))
            {
                _ = session.Advanced.Lazily.Load<Doc>(docId).Value;
                return session.Advanced.NumberOfRequests;
            }
        }
        
        public class Doc
        {
            public string Version { get; set; }
        }
    }
}
