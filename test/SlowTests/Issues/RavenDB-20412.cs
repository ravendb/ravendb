using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.OngoingTasks;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20412 : ReplicationTestBase
    {
        public RavenDB_20412(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Save_Tombstones_On_Disabled_External_Replication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var user = new User { Name = "Yonatan" };
                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store1);

                const string documentId = "users/1-A";
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(user, documentId);
                    await session.SaveChangesAsync();
                }

                var externalList1 = await SetupReplicationAsync(store1, store2);
                WaitForDocumentToReplicate<User>(store2, documentId, 3000);

                await store1.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(externalList1.First().TaskId, OngoingTaskType.Replication, disable: true));

                using (var session = store1.OpenAsyncSession())
                {
                    session.Delete(user.Id);
                    await session.SaveChangesAsync();
                }

                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                await store1.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(externalList1.First().TaskId, OngoingTaskType.Replication, disable: false));

                Assert.True(WaitForDocumentDeletion(store2, documentId, 3000));
            }
        }

    }
}
