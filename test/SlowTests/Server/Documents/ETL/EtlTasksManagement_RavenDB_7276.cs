using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class EtlTasksManagement_RavenDB_7276 : EtlTestBase
    {
        public EtlTasksManagement_RavenDB_7276(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDeleteEtl()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = "aaa",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "S1",
                            Collections = {"Users"}
                        }
                    }
                };

                var result = AddEtl(store, configuration, new RavenConnectionString
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
        public void CanUpdateEtl()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = "aaa",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "S1",
                            Collections = {"Users"}
                        },
                        new Transformation
                        {
                            Name = "S2",
                            Collections = {"Users"}
                        }
                    }
                };

                var result = AddEtl(store, configuration, new RavenConnectionString
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
        public void CanDisableEtl()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = "aaa",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "S1",
                            Collections = {"Users"}
                        }
                    }
                };

                var result = AddEtl(store, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                });

                var toggleResult = store.Maintenance.Send(new ToggleOngoingTaskStateOperation(result.TaskId, OngoingTaskType.RavenEtl, true));
                Assert.NotNull(toggleResult);
                Assert.True(toggleResult.RaftCommandIndex > 0);
                Assert.True(toggleResult.TaskId > 0);

                var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(result.TaskId, OngoingTaskType.RavenEtl));
                Assert.Equal(OngoingTaskState.Disabled, ongoingTask.TaskState);
            }
        }
    }
}
