using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;
using Size = Sparrow.Size;
using TimeoutException = System.TimeoutException;

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
                var amre = new AsyncManualResetEvent();
                var observable = store.Changes().ForDocument(docId);
                await observable.EnsureSubscribedNow();
                observable.Subscribe(_ => amre.Set());

                using var session = store.OpenAsyncSession();
                await session.StoreAsync(new Doc { Version = "2" }, docId);
                await session.SaveChangesAsync();

                await amre.WaitAsync(TimeSpan.FromSeconds(30));
            }

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
            AggressiveCacheMode aggressiveCacheMode,
            bool createVersion2 = true)
        {
            const string docId = "doc-1";

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
                var amre = new AsyncManualResetEvent();
                var observable = store.Changes().ForDocument(docId);
                await observable.EnsureSubscribedNow();
                observable.Subscribe(_ => amre.Set());

                using var session = store.OpenAsyncSession();
                await session.StoreAsync(new Doc { Version = "2" }, docId);
                await session.SaveChangesAsync();

                await amre.WaitAsync(TimeSpan.FromSeconds(30));
            }

            using (var session = store.OpenSession())
            using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5), aggressiveCacheMode))
            {
                _ = session.Advanced.Lazily.Load<Doc>(docId).Value;
                return session.Advanced.NumberOfRequests;
            }
        }

        public class Doc
        {
            public string Version { get; set; }
        }

        [Fact]
        public async Task LazilyLoad_WhenOneOfLoadedIsCachedAndNotModified_ShouldNotBeNull()
        {
            const string cachedId = "TestObjs/cached";
            const string notCachedId = "TestObjs/notCached";

            using var store = GetDocumentStore();
            store.AggressivelyCacheFor(TimeSpan.FromMinutes(5));
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj(), cachedId);
                await session.StoreAsync(new TestObj(), notCachedId);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                //Load to cache
                var a = await session.Advanced.Lazily.LoadAsync<TestObj>(cachedId).Value;
            }

            using (var session = store.OpenAsyncSession())
            {
                var lazy = session.Advanced.Lazily.LoadAsync<TestObj>(cachedId);
                _ = await session.Advanced.Lazily.LoadAsync<TestObj>(notCachedId).Value;
                Assert.NotNull(await lazy.Value);
            }

        }

        [Fact]
        public async Task LazilyLoad_WhenSessionTrackResultFromFreedCacheItems_ShouldUseUnfreedMemory()
        {
            const int docsCount = 50;


            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore => documentStore.Conventions.MaxHttpCacheSize = new Size(1, SizeUnit.Megabytes)
            });

            await using (var bulk = store.BulkInsert())
            {
                var random = new Random();

                for (int i = 0; i < docsCount; i++)
                {
                    await bulk.StoreAsync(new TestObj
                    {
                        LargeContent = string.Create(18000, (object)null,
                            (chars, _) =>
                            {
                                foreach (ref char c in chars)
                                {
                                    c = (char)random.Next(char.MaxValue);
                                }
                            })
                    }, $"TestObjs/{i}");
                }
            }

            //Cache results
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 15; i++)
                {
                    _ = session.Advanced.Lazily.LoadAsync<TestObj>($"TestObjs/{i}");
                }
                await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = docsCount + 30;

                for (int i = 0; i < docsCount; i++)
                {
                    var lazy = session.Advanced.Lazily.LoadAsync<TestObj>($"TestObjs/{i}");
                    await lazy.Value;
                }

                //Used to fail here if used memory of freed cache items
                await session.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task LazilyLoad_WhenTheCachedCleanedBeforeUsingTheResults_ShouldGetUnfreedResults()
        {
            var random = new Random(0);
            string largeContent = string.Create(30000, (object)null,
                (chars, _) =>
                {
                    foreach (ref char c in chars)
                    {
                        c = (char)random.Next(char.MaxValue);
                    }
                });

            var conventionsMaxHttpCacheSize = new Size(1, SizeUnit.Megabytes);

            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Conventions.MaxHttpCacheSize = conventionsMaxHttpCacheSize;
                }
            });
            store.AggressivelyCache();

            async Task<int> GetSingleDocSize()
            {
                const string id = "TestObjs/sizeCheck";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new TestObj { LargeContent = largeContent }, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<BlittableJsonReaderObject>(id);
                    return doc.Size;
                }
            }
            int singleDocSize = await GetSingleDocSize();

            var fullCacheDocCount = (int)(conventionsMaxHttpCacheSize.GetValue(SizeUnit.Bytes) / singleDocSize);

            int docCount = 4 * fullCacheDocCount;
            await using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < docCount; i++)
                {
                    await bulk.StoreAsync(new TestObj { LargeContent = largeContent }, $"TestObjs/{i}");
                }
            }

            int safetyRange = (int)(fullCacheDocCount * 0.2); //To make sure we don't exceed the max cache size and free the cached items
            int cachedCount = fullCacheDocCount - safetyRange;
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < cachedCount; i++)
                {
                    session.Advanced.Lazily.LoadAsync<TestObj>($"TestObjs/{i}");
                }
                await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var requests =
                    Enumerable.Range(0, cachedCount)
                        .Select(i => new GetRequest { Url = "/docs", Query = $"?&id={Uri.EscapeDataString($"TestObjs/{i}")}", });

                var multiGetOperation = new MultiGetOperation((AsyncDocumentSession)session);
                using var multiGetCommand = multiGetOperation.CreateRequest(requests.ToList());

                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
                {
                    await requestExecutor.ExecuteAsync(multiGetCommand, context).ConfigureAwait(false);

                    using (var session2 = store.OpenAsyncSession())
                    {
                        session2.Advanced.MaxNumberOfRequestsPerSession = 1000;

                        for (var i = cachedCount; i < docCount; i++)
                        {
                            _ = session2.Advanced.Lazily.LoadAsync<TestObj>($"TestObjs/{i}");
                        }
                        await session2.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();
                    }

                    foreach (var getResponse in multiGetCommand.Result)
                    {
                        var blittable = (BlittableJsonReaderObject)getResponse.Result;
                        blittable.BlittableValidation();
                    }
                }

                await session.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task LazilyLoad_WhenUseNotFoundCacheItem_ShouldUseItReleaseAtTheEnd()
        {
            const string notExistDocId = "NotExistDocId";

            using (var store = GetDocumentStore())
            {
                store.AggressivelyCache();

                using (var session = store.OpenAsyncSession())
                {
                    //Add "NotExistDocId" to cache
                    await session.Advanced.Lazily.LoadAsync<TestObj>(notExistDocId).Value;
                }

                using (var session = store.OpenAsyncSession())
                {
                    //Add "NotExistDocId" to cache
                    await session.Advanced.Lazily.LoadAsync<TestObj>(notExistDocId).Value;
                    Assert.Equal(0, session.Advanced.NumberOfRequests);
                }
            }
            //If not all of the `HttpCacheItem`s were released we will get an exception in the `HttpCacheItem` finalizer
        }

        [Fact]
        public async Task LazilyLoad_WhenQueryForNotFoundNotModified_ShouldUseCache()
        {
            const string notExistDocId = "NotExistDocId";

            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                //Add "NotExistDocId" to cache
                await session.Advanced.Lazily.LoadAsync<TestObj>(notExistDocId).Value;
            }

            var requestExecutor = store.GetRequestExecutor();
            using (var session = store.OpenAsyncSession())
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var multiGetOperation = new MultiGetOperation((AsyncDocumentSession)session);
                var requests = new List<GetRequest>
                {
                    new GetRequest { Url = "/docs", Query = $"?&id={Uri.EscapeDataString(notExistDocId)}" },
                };
                using var multiGetCommand = multiGetOperation.CreateRequest(requests);

                //Should use the cache here and release it in after that
                await requestExecutor.ExecuteAsync(multiGetCommand, context).ConfigureAwait(false);
                Assert.Equal(HttpStatusCode.NotModified, multiGetCommand.Result.First().StatusCode);
            }
        }

        public class TestObj
        {
            public string Id { get; set; }
            public string LargeContent { get; set; }
        }

        public class NewTest { }
    }
}
