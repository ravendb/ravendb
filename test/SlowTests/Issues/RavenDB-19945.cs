using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues ;

public class RavenDB_19945 : RavenTestBase
{
    public RavenDB_19945(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task WillNotSendEvenForEachChangeForAggressiveCaching(Options options)
    {
        using var store = GetDocumentStore(options);

        const int count = 100;
        var cde = new CountdownEvent(count);
        var requestExecutor = store.GetRequestExecutor();
        int cacheGeneration = requestExecutor.Cache.Generation;
        using(await store.AggressivelyCacheAsync())
        {
            IDatabaseChanges databaseChanges = store.Changes();
            await databaseChanges.EnsureConnectedNow().ConfigureAwait(false);
            IChangesObservable<DocumentChange> forAllDocuments = databaseChanges.ForAllDocuments();
            await forAllDocuments.EnsureSubscribedNow().ConfigureAwait(false);
            forAllDocuments.Subscribe(_ => cde.Signal());

            for (int i = 0; i < count / 10; i++)
            {
                using var s = store.OpenAsyncSession();
                for (int j = 0; j < count/10; j++)
                {
                    await s.StoreAsync(new { });
                }
                await s.SaveChangesAsync();
            }
        }

        Assert.True(cde.Wait(TimeSpan.FromSeconds(30)));
        
        Assert.NotEqual(cacheGeneration, requestExecutor.Cache.Generation); // has updates
        Assert.True(cacheGeneration + count > requestExecutor.Cache.Generation); // not for every single one
    }
}
