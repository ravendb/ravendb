using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nest;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Changes;

public class BasicChangesIndexesTests : RavenTestBase
{
    public BasicChangesIndexesTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ChangesApi)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Can_Subscribe_To_Single_Index_Changes(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var changes = store.Changes())
            {
                await changes.EnsureConnectedNow();

                var index1 = new Index1();
                var index2 = new Index2();

                var index1Cde = new CountdownEvent(1);
                var index2Cde = new CountdownEvent(1);

                var index1Observable = changes.ForIndex(index1.IndexName);
                var index2Observable = changes.ForIndex(index2.IndexName);

                index1Observable.Subscribe(change => index1Cde.Signal());
                index2Observable.Subscribe(change => index2Cde.Signal());

                await index1Observable.EnsureSubscribedNow();
                await index2Observable.EnsureSubscribedNow();

                await index1.ExecuteAsync(store);
                await index2.ExecuteAsync(store);

                Assert.True(index1Cde.Wait(TimeSpan.FromSeconds(30)));
                Assert.True(index2Cde.Wait(TimeSpan.FromSeconds(30)));
            }
        }
    }

    [RavenTheory(RavenTestCategory.ChangesApi)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Can_Subscribe_To_All_Index_Changes(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var changes = store.Changes())
            {
                await changes.EnsureConnectedNow();

                var index1 = new Index1();
                var index2 = new Index2();

                var indexCde = new CountdownEvent(2);

                var indexObservable = changes.ForAllIndexes();

                indexObservable.Subscribe(change => indexCde.Signal());

                await indexObservable.EnsureSubscribedNow();

                await index1.ExecuteAsync(store);
                await index2.ExecuteAsync(store);

                Assert.True(indexCde.Wait(TimeSpan.FromSeconds(30)));
            }
        }
    }

    private class Index1 : AbstractIndexCreationTask<Product>
    {
        public Index1()
        {
            Map = products => from p in products
                              select new
                              {
                                  Name = p.Name
                              };
        }
    }

    private class Index2 : AbstractIndexCreationTask<Company>
    {
        public Index2()
        {
            Map = companies => from c in companies  
                select new
                {
                    Name = c.Name
                };
        }
    }
}
