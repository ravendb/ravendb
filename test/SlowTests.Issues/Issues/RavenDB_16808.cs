using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_16808 : RavenTestBase
    {
        public RavenDB_16808(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task ShouldIncrementOnlySessionAdvancedNumberOfRequests()
        {
            using DocumentStore store = GetDocumentStore();

            RequestExecutor requestExecutor = store.GetRequestExecutor();
            TestObj entity = new TestObj();
            using (IAsyncDocumentSession session = store.OpenAsyncSession())
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            using (IAsyncDocumentSession session = store.OpenAsyncSession())
            {
                _ = await session.LoadAsync<TestObj>(entity.Id);
            }

            using (IAsyncDocumentSession session = store.OpenAsyncSession())
            {
                using (await store.AggressivelyCacheForAsync(TimeSpan.MaxValue, AggressiveCacheMode.DoNotTrackChanges))
                {
                    long reBefore = requestExecutor.NumberOfServerRequests;
                    int sessionBefore = session.Advanced.NumberOfRequests;
                    _ = await session.LoadAsync<TestObj>(entity.Id);
                    long reForLoad = requestExecutor.NumberOfServerRequests - reBefore; // We took the value from cache
                    int sessionForLoad = session.Advanced.NumberOfRequests - sessionBefore;
                    RavenTestHelper.AssertAll(() => $"reBefore:{reBefore}, reForLoad:{reForLoad}, sessionBefore:{sessionBefore}, sessionForLoad:{sessionForLoad}",
                        () => Assert.Equal(1, sessionForLoad), // We requested the value 
                        () => Assert.Equal(0, reForLoad)); // We took it from the cache
                }
            }
        }
        
        private class TestObj
        {
            public string Id { get; set; }
            public string LargeContent { get; set; }
        }
    }
}
