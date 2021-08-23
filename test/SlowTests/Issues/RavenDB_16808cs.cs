using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using SlowTests.Bugs.Caching;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16808 : RavenTestBase
    {
        public RavenDB_16808(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldIncrementOnlySessionAdvancedNumberOfRequests()
        {
            using DocumentStore store = GetDocumentStore();

            RequestExecutor requestExecutor = store.GetRequestExecutor();
            UseCachingInLazyTests.TestObj entity = new UseCachingInLazyTests.TestObj();
            using (IAsyncDocumentSession session = store.OpenAsyncSession())
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            using (IAsyncDocumentSession session = store.OpenAsyncSession())
            {
                _ = await session.LoadAsync<UseCachingInLazyTests.TestObj>(entity.Id);
            }

            using (IAsyncDocumentSession session = store.OpenAsyncSession())
            {
                using (store.AggressivelyCacheFor(TimeSpan.MaxValue, AggressiveCacheMode.DoNotTrackChanges))
                {
                    long reBefore = requestExecutor.NumberOfServerRequests;
                    int sessionBefore = session.Advanced.NumberOfRequests;
                    _ = await session.LoadAsync<UseCachingInLazyTests.TestObj>(entity.Id);
                    long reForLoad = requestExecutor.NumberOfServerRequests - reBefore; // We took the value from cache
                    int sessionForLoad = session.Advanced.NumberOfRequests - sessionBefore;
                    RavenTestHelper.AssertAll(() => $"reBefore:{reBefore}, reForLoad:{reForLoad}, sessionBefore:{sessionBefore}, sessionForLoad:{sessionForLoad}",
                        () => Assert.Equal(1, sessionForLoad), // We requested the value 
                        () => Assert.Equal(0, reForLoad)); // We took it from the cache
                }
            }
        }
    }
}
