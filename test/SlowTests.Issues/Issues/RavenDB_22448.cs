using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_22448 : RavenTestBase
{
    public RavenDB_22448(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task ConnectionStringChanges_WillRestartEtl()
    {
        using (var store = GetDocumentStore())
        {
            store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = "cs", TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" }, Database = "Northwind",
            }));

            var etlConfiguration = new RavenEtlConfiguration()
            {
                Name = "test",
                ConnectionStringName = "cs",
                Transforms = { new Transformation { Name = "loadAll", Collections = { "Users" }, Script = "loadToUsers(this)" } }
            };

            var addRavenEtlResult = store.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration));

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            var mre = new ManualResetEventSlim();
            database.EtlLoader.ProcessRemoved += process => mre.Set();
            
            var ravenEtl = (RavenEtl)database.EtlLoader.Processes.SingleOrDefault(x => x.TaskId == addRavenEtlResult.TaskId);
            
            store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = "cs", TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" }, Database = "Northwind2",
            }));
            
            Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));
            
            var ravenEtl2 = (RavenEtl)database.EtlLoader.Processes.SingleOrDefault(x => x.TaskId == addRavenEtlResult.TaskId);
            
            Assert.NotEqual(ravenEtl, ravenEtl2);
            Assert.False(ravenEtl.Configuration.Connection.IsEqual(ravenEtl2.Configuration.Connection));
        }
    }
}
