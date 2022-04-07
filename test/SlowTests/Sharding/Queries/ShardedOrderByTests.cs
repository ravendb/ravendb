using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Sorters;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Exceptions;
using SlowTests.Core.Utils.Entities;
using SlowTests.Issues;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Queries
{
    public class ShardedOrderByTests : RavenTestBase
    {
        public ShardedOrderByTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        public async Task ThrowsOnCustomSortersUsage()
        {
            DoNotReuseServer();

            var error = Assert.Throws<RavenException>(() =>
            {
                using (Sharding.GetDocumentStore(new Options
                       {
                           ModifyDatabaseRecord = record => record.Sorters = new Dictionary<string, SorterDefinition>
                           {
                               {
                                   "MySorter", new SorterDefinition
                                   {
                                       Name = "MySorter",
                                       Code = "Code"
                                   }
                               }
                           }
                       }))
                {

                }
            });

            Assert.Contains("Custom sorting is not supported in sharding as of yet", error.Message);

            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var err = await Assert.ThrowsAsync<RavenException>(async () =>
                        await session
                            .Advanced
                            .AsyncRawQuery<Company>("from Companies order by custom(Name, 'MySorter')")
                            .ToListAsync());

                    Assert.Contains("Custom sorting is not supported in sharding as of yet", err.Message);
                }
            }

            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "C1" });
                    await session.StoreAsync(new Company { Name = "C2" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var err = await Assert.ThrowsAsync<RavenException>(async () =>
                        await store.Maintenance.SendAsync(new PutSortersOperation(new SorterDefinition
                        {
                            Name = "MySorter",
                            Code = "Code"
                        })));

                    Assert.Contains("Custom sorting is not supported in sharding as of yet", err.Message);
                }
            }
        }
    }
}
