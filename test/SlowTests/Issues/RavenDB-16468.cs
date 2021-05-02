using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16468 : RavenTestBase
    {
        public RavenDB_16468(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_query_after_the_superseded_auto_index_was_deleted()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.TimeBeforeDeletionOfSupersededAutoIndex)] = "0";
                }
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha",
                        LastName = "Kotler"
                    });
                    await session.SaveChangesAsync();
                }

                var database = await GetDatabase(store.Database);
                var mreAfterFirstIndexCreation = new ManualResetEvent(false);
                var mreAfterSecondQuery = new ManualResetEvent(false);

                database.IndexStore.ForTestingPurposesOnly().AfterIndexCreation += indexName =>
                {
                    if (indexName != "Auto/Users/ByName")
                        return;

                    mreAfterFirstIndexCreation.Set();
                    mreAfterSecondQuery.WaitOne();
                };

                var nameQuery = NameQuery();

                async Task NameQuery()
                {
                    // will create an auto index on the Name field
                    using (var session = store.OpenAsyncSession())
                    {
                        var result = await session.Query<User>()
                            .Statistics(out var stats)
                            .Where(x => x.Name == "Grisha")
                            .ToListAsync();

                        Assert.Equal("Auto/Users/ByLastNameAndName", stats.IndexName);
                        Assert.Equal(1, result.Count);
                        Assert.Equal("Grisha", result[0].Name);
                    }
                }

                mreAfterFirstIndexCreation.WaitOne();

                using (var session = store.OpenAsyncSession())
                {
                    // will create an auto index on the Name and LastName fields
                    var result = await session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.LastName == "Kotler")
                        .ToListAsync();

                    Assert.Equal(1, result.Count);
                    Assert.Equal("Kotler", result[0].LastName);

                    mreAfterSecondQuery.Set();
                }

                await nameQuery;
            }
        }
    }
}
