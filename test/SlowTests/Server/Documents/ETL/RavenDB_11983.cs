using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_11983 : EtlTestBase
    {
        [Fact]
        public async Task Should_have_process_per_transformation_script()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var notifications = new AsyncQueue<DynamicJsonValue>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    AddEtl(store, new RavenEtlConfiguration()
                    {
                        ConnectionStringName = "test",
                        Name = "myFirstEtl",
                        Transforms =
                        {
                            new Transformation()
                            {
                                Collections = {"Users"},
                                Script = "loadToUsers(this)",
                                Name = "a"
                            },
                            new Transformation()
                            {
                                Collections = {"Addresses"},
                                Script = "loadToAddresses(this)",
                                Name = "b"
                            }
                        }
                    }, new RavenConnectionString()
                    {
                        Name = "test",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                        Database = "Northwind",
                    });

                    Assert.Equal(2, database.EtlLoader.Processes.Length);

                    AddEtl(store, new RavenEtlConfiguration()
                    {
                        ConnectionStringName = "test",
                        Name = "mySecondEtl",
                        Transforms =
                        {
                            new Transformation()
                            {
                                Collections = {"Users"},
                                Script = "loadToUsers(this)",
                                Name = "a"
                            },
                            new Transformation()
                            {
                                Collections = {"Addresses"},
                                Script = "loadToAddresses(this)",
                                Name = "b"
                            }
                        }
                    }, new RavenConnectionString()
                    {
                        Name = "test",
                        TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                        Database = "Northwind",
                    });

                    Assert.Equal(4, database.EtlLoader.Processes.Length);
                }
            }
        }
    }
}
