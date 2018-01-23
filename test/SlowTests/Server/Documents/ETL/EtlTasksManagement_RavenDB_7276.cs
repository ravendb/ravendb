using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class EtlTasksManagement_RavenDB_7276 : EtlTestBase
    {
        [Fact]
        public async Task CanDeleteEtl()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "aaa",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Collections = {"Users"}
                        }
                    }
                };

                var result = AddEtl(store, configuration, new RavenConnectionString()
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                });

                store.Maintenance.Send(new DeleteOngoingTaskOperation(result.TaskId, OngoingTaskType.RavenEtl));

                var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(result.TaskId, OngoingTaskType.RavenEtl));

                Assert.Null(ongoingTask);
            }
        }

        [Fact]
        public async Task CanUpdateEtl()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "aaa",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Collections = {"Users"}
                        },
                        new Transformation()
                        {
                            Collections = {"Users"}
                        }
                    }
                };

                var result = AddEtl(store, configuration, new RavenConnectionString()
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                });

                configuration.Transforms[0].Disabled = true;

                var update = store.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(result.TaskId, configuration));

                var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(update.TaskId, OngoingTaskType.RavenEtl));

                Assert.Equal(OngoingTaskState.PartiallyEnabled, ongoingTask.TaskState);
            }
        }

        [Fact]
        public async Task CanDisableEtl()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "aaa",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Collections = {"Users"}
                        }
                    }
                };

                var result = AddEtl(store, configuration, new RavenConnectionString()
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                });


                store.Maintenance.Send(new ToggleOngoingTaskStateOperation(result.TaskId, OngoingTaskType.RavenEtl, true));

                var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(result.TaskId, OngoingTaskType.RavenEtl));

                Assert.Equal(OngoingTaskState.Disabled, ongoingTask.TaskState);
            }
        }
    }
}
