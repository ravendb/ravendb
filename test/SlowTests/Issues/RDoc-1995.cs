using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CompareExchange;
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

                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User {Name = "Grisha"}, id);
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, 3);
                    await session.SaveChangesAsync();
                }

                await Migrate(store1, store2);

                using (var session = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);

                    var cmpValue = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id);
                    Assert.NotNull(cmpValue);
                }

                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Delete(id);
                    var cmpValue = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id);
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(id, cmpValue.Index);
                    await session.SaveChangesAsync();
                }

                await Migrate(store1, store2);

                using (var session = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Null(user);

                    var cmpValue = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id);
                    Assert.Null(cmpValue);
                }
            }
        }

        [Fact]
        public async Task Can_Live_Import_CompareExchange_Incremental()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                const string id1 = "test1";

                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id1, 3);
                    await session.SaveChangesAsync();
                }

                await Migrate(store1, store2);

                using (var session = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpValue = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id1);
                    Assert.NotNull(cmpValue);
                }

                using (var session = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpValue = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id1);
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(id1, cmpValue.Index);
                    await session.SaveChangesAsync();
                }

                await Migrate(store1, store2);

                using (var session = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpValue = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id1);
                    Assert.Null(cmpValue);
                }

                const string id2 = "test2";
                using (var session = store1.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id2, 5);
                    await session.SaveChangesAsync();
                }

                await Migrate(store1, store2);

                using (var session = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpValue1 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id1);
                    Assert.Null(cmpValue1);

                    var cmpValue2 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(id2);
                    Assert.NotNull(cmpValue2);
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
