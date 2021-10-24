using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Smuggler.Migration;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDoc_1995 : RavenTestBase
    {
        public RDoc_1995(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Live_Import_Tombstones()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                const string id = "test";

                using (var session1 = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session1.StoreAsync(new User {Name = "Grisha"}, id);
                    session1.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, 3);
                    await session1.SaveChangesAsync();
                }

                await Migrate(store1, store2);

                using (var session2 = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = await session2.LoadAsync<User>(id);
                    Assert.NotNull(user);

                    var cmpValue = await session2.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id);
                    Assert.NotNull(cmpValue);
                }

                using (var session1 = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session1.Delete(id);
                    var cmpValue = await session1.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id);
                    session1.Advanced.ClusterTransaction.DeleteCompareExchangeValue(id, cmpValue.Index);
                    await session1.SaveChangesAsync();
                }

                await Migrate(store1, store2);

                using (var session2 = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = await session2.LoadAsync<User>(id);
                    Assert.Null(user);

                    var cmpValue = await session2.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id);
                    Assert.Null(cmpValue);
                }
            }
        }

        private async Task Migrate(DocumentStore store1, DocumentStore store2)
        {
            var migrate = new Migrator(
                new DatabasesMigrationConfiguration
                {
                    ServerUrl = Server.WebUrl,
                    Databases = new List<DatabaseMigrationSettings>
                    {
                        new DatabaseMigrationSettings {DatabaseName = store1.Database, OperateOnTypes = DatabaseItemType.Documents,}
                    }
                }, Server.ServerStore);

            await migrate.UpdateBuildInfoIfNeeded();

            var operationId =
                migrate.StartMigratingSingleDatabase(
                    new DatabaseMigrationSettings {DatabaseName = store1.Database, OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.CompareExchange,},
                    GetDocumentDatabaseInstanceFor(store2).Result);

            WaitForValue(() =>
            {
                var operation = store2.Maintenance.Send(new GetOperationStateOperation(operationId));
                return operation.Status == OperationStatus.Completed;
            }, true);
        }
    }
}
