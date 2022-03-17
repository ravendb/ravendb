using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Queries;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9680 : RavenTestBase
    {
        public RavenDB_9680(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_delete_index_entries_on_document_deletion_after_database_restart()
        {
            using (var store = GetDocumentStore(new Options() { Path = NewDataPath() }))
            {
                string indexName;

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "joe"
                    }, "users/1");

                    session.Store(new User()
                    {
                        Name = "doe"
                    }, "users/2");

                    session.SaveChanges();

                    // to create an index
                    session.Query<User>().Customize(x => x.WaitForNonStaleResults()).Statistics(out var stats).Where(x => x.Name != null).ToList();

                    indexName = stats.IndexName;
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                await (await GetDatabase(store.Database)).TombstoneCleaner.ExecuteCleanup(); // will delete users/1 tombstone

                // the restart is necessary to expose the issue
                // on db load we read last etag
                // however next created tombstone will get _the same_ etag as tombstone of users/1
                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                using (var session = store.OpenSession())
                {
                    session.Delete("users/2");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                
                using (var commands = store.Commands())
                {
                    var result = commands.Query(new IndexQuery { Query = $"from index '{indexName}'" }, indexEntriesOnly: true);

                    Assert.Equal(0, result.Results.Length);
                }
            }
        }
    }
}
