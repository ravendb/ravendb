using System.Threading.Tasks;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.ETL;
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
                    Url = "http://127.0.0.1:8080",
                    Database = "Northwind",
                });

                store.Admin.Server.Send(new DeleteOngoingTaskOperation(database.Name, result.TaskId, OngoingTaskType.RavenEtl));
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
                        }
                    }
                };

                var result = AddEtl(store, configuration, new RavenConnectionString()
                {
                    Name = "test",
                    Url = "http://127.0.0.1:8080",
                    Database = "Northwind",
                });

                configuration.Transforms[0].Disabled = true;

                store.Admin.Server.Send(new UpdateEtlOperation<RavenConnectionString>(result.TaskId, configuration, database.Name));
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
                    Url = "http://127.0.0.1:8080",
                    Database = "Northwind",
                });


                store.Admin.Server.Send(new ToggleTaskStateOperation(database.Name , result.TaskId, OngoingTaskType.RavenEtl, true));
            }
        }
    }
}