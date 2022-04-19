using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;
using Order = Tests.Infrastructure.Entities.Order;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class RavenDB_18465 : EtlTestBase
    {
        public RavenDB_18465(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task EtlProcessStateShouldBeDeletedAfterTaskRemoval()
        {
            const string script = @"
var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadTo(""Orders"", partitionBy(key),
    {
        Company : this.Company,
        ShipVia : this.ShipVia
    });
";
            using (var store = GetDocumentStore())
            {
                await InsertData(store);

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
                var path = NewDataPath(forceCreateDir: true);

                const string configurationName = "olap-test";
                const string transformationName = "transformation-test";
                var result = LocalTests.SetupLocalOlapEtl(store, script, path, name: configurationName, transformationName: transformationName);

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                var files = Directory.GetFiles(path, searchPattern: LocalTests.AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(2, files.Length);

                var key = EtlProcessState.GenerateItemName(store.Database, configurationName, transformationName).ToLowerInvariant();

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var item = Server.ServerStore.Engine.StateMachine.GetItem(context, key);
                    Assert.NotNull(item);
                }

                // delete task
                store.Maintenance.Send(new DeleteOngoingTaskOperation(result.TaskId, OngoingTaskType.OlapEtl));
                var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(result.TaskId, OngoingTaskType.OlapEtl));
                Assert.Null(ongoingTask);

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var item = Server.ServerStore.Engine.StateMachine.GetItem(context, key);
                    Assert.Null(item);
                }
            }
        }

        private async Task InsertData(IDocumentStore store)
        {
            var baseline = new DateTime(2020, 1, 1);

            using (var session = store.OpenAsyncSession())
            {
                const int numberOfDaysInJanuary = 31;
                const int numberOfDaysInFebruary = 28;

                for (int i = 0; i < numberOfDaysInJanuary; i++)
                {
                    await session.StoreAsync(new Order
                    {
                        Id = $"orders/{i}",
                        OrderedAt = baseline.AddDays(i),
                        ShipVia = $"shippers/{i}",
                        Company = $"companies/{i}"
                    });
                }

                for (int i = 0; i < numberOfDaysInFebruary; i++)
                {
                    var next = i + numberOfDaysInJanuary;
                    await session.StoreAsync(new Order
                    {
                        Id = $"orders/{next}",
                        OrderedAt = baseline.AddMonths(1).AddDays(i),
                        ShipVia = $"shippers/{next}",
                        Company = $"companies/{next}"
                    });
                }

                await session.SaveChangesAsync();
            }
        }
    }

}
