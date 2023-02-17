using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_18166 : RavenTestBase
{
    public RavenDB_18166(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public async Task ShouldMarkQueryResultsAsStaleIfShardsHaveDifferentAutoIndexes()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                {
                    session.Store(new User { Name = "Joe" });
                    session.Store(new User { Name = "Doe" });
                }

                session.SaveChanges();

                var results = session.Query<User>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Name == "Joe")
                    .ToList();


                Assert.Equal(100, results.Count);
            }

            var shardedDb = await Sharding.GetShardsDocumentDatabaseInstancesFor(store).FirstAsync();

            Index index = shardedDb.IndexStore.GetIndex("Auto/Users/ByName");
            
            Assert.NotEqual(0, index.Definition.ClusterState.LastIndex); // precaution - to make sure we have real value here
            
            // let's pretend that on one shard the definition is different
            index.Definition.ClusterState.LastIndex++;
            
            using (var session = store.OpenSession())
            {
                var results = session.Query<User>()
                    .Statistics(out var stats)
                    .Where(x => x.Name == "Joe")
                    .ToList();

                Assert.True(stats.IsStale);
            }
        }
    }

    [RavenFact(RavenTestCategory.Querying)]
    public async Task ShouldMarkQueryResultsAsStaleIfShardsHaveDifferentStaticIndexes()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            await new Users_ByName().ExecuteAsync(store);

            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 100; i++)
                {
                    session.Store(new User { Name = "Joe" });
                    session.Store(new User { Name = "Doe" });
                }

                session.SaveChanges();

                var results = session.Query<User, Users_ByName>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Name == "Joe")
                    .ToList();


                Assert.Equal(100, results.Count);
            }

            var shardedDb = await Sharding.GetShardsDocumentDatabaseInstancesFor(store).FirstAsync();

            Index index = shardedDb.IndexStore.GetIndex("Users/ByName");

            Assert.NotEqual(0, index.Definition.ClusterState.LastIndex); // precaution - to make sure we have real value here

            // let's pretend that on one shard the definition is different
            index.Definition.ClusterState.LastIndex++;

            using (var session = store.OpenSession())
            {
                var results = session.Query<User, Users_ByName>()
                    .Statistics(out var stats)
                    .Where(x => x.Name == "Joe")
                    .ToList();

                Assert.True(stats.IsStale);
            }
        }
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new {u.Name};
        }
    }
}
