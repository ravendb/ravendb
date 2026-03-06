using FastTests;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25419 : RavenTestBase
{
    public RavenDB_25419(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void RenamingEtlShouldThrow()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            var connectionString = new RavenConnectionString()
            {
                Name = "ConnectionString",
                Database = dest.Database,
                TopologyDiscoveryUrls = dest.Urls
            };
            
            var transformation = new Transformation
            {
                Name = "Transformation1",
                Collections = ["Users"],
                Script = """
                         this.Name = 'James Doe';
                         loadToUsers(this);
                         """,
                ApplyToAllDocuments = false,
                Disabled = false
            };
            
            var configuration = new RavenEtlConfiguration
            {
                Name = "Task1",
                ConnectionStringName = "ConnectionString",
                Transforms =
                {
                    transformation
                }
            };
            
            var putConnectionStringResult = src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
            Assert.NotNull(putConnectionStringResult.RaftCommandIndex);

            var addEtlOperationResult = src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(configuration));
            Assert.NotNull(addEtlOperationResult.RaftCommandIndex);
            
            configuration.Name = "Task2";

            var ex = Assert.Throws<RavenException>(() => src.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(addEtlOperationResult.TaskId, configuration)));
            Assert.Contains("Changing Name of ETL is not supported", ex.Message);
        }
    }
}
