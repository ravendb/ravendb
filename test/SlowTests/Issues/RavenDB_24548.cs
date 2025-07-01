using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24548 : RavenTestBase
    {
        public RavenDB_24548(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
        public async Task CanStartDatabaseWithSharedJournalWithoutErrors()
        {
            var path = NewDataPath();
            var name = GetDatabaseName();
            string shardJournalPath;
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseName =_ => name,
                       Path = path,
                       DeleteDatabaseOnDispose = false
                   }))
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                shardJournalPath = database.IndexStore.SharedJournals.Env.Options.BasePath.FullPath;
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.Indexes));
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: false));
            }

            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseName =_ => name,
                       Path = path,
                   }))
            {
                Assert.False(Server.ServerStore.DatabasesLandlord.CatastrophicFailureHandler.ErrorAtPath(shardJournalPath));
            }
        }
    }
}
