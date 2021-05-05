using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class BackupTests : EtlTestBase
    {
        public BackupTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanExportAndImportOlapEtl()
        {
            var script = @"
var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key),
    {
        Company : this.Company,
        ShipVia : this.ShipVia
    });
";

            var path = NewDataPath();

            using (var source = GetDocumentStore())
            using (var destination1 = GetDocumentStore())
            using (var destination2 = GetDocumentStore())
            using (var destination3 = GetDocumentStore())
            {
                SetupLocalOlapEtl(source, script, path);

                var operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), source.Smuggler.ForDatabase(destination1.Database));
                await operation.WaitForCompletionAsync();

                var sourceRecord = await source.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(source.Database));
                var destinationRecord1 = await source.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(destination1.Database));

                Assert.Equal(sourceRecord.OlapEtls.Count, destinationRecord1.OlapEtls.Count);
                Assert.Equal(sourceRecord.OlapConnectionStrings.Count, destinationRecord1.OlapConnectionStrings.Count);

                var sourceOlapEtl = sourceRecord.OlapEtls[0];
                var destinationOlapEtl = destinationRecord1.OlapEtls[0];

                Assert.False(sourceOlapEtl.Disabled);
                Assert.True(destinationOlapEtl.Disabled);
                Assert.NotEqual(sourceOlapEtl.TaskId, destinationOlapEtl.TaskId);

                using (var session = source.OpenSession())
                {
                    destinationOlapEtl.Disabled = sourceOlapEtl.Disabled;
                    destinationOlapEtl.TaskId = sourceOlapEtl.TaskId;
                    var sourceOlapEtlJson = source.Conventions.Serialization.DefaultConverter.ToBlittable(sourceOlapEtl, session.Advanced.Context);
                    var destinationOlapEtlJson = source.Conventions.Serialization.DefaultConverter.ToBlittable(destinationOlapEtl, session.Advanced.Context);

                    var changed = BlittableOperation.EntityChanged(destinationOlapEtlJson, new DocumentInfo { Id = "", Document = sourceOlapEtlJson }, changes: null);
                    Assert.False(changed);

                    var sourceOlapConnectionStringJson = source.Conventions.Serialization.DefaultConverter.ToBlittable(sourceRecord.OlapConnectionStrings.Values.First(), session.Advanced.Context);
                    var destinationOlapConnectionStringJson = source.Conventions.Serialization.DefaultConverter.ToBlittable(destinationRecord1.OlapConnectionStrings.Values.First(), session.Advanced.Context);

                    changed = BlittableOperation.EntityChanged(destinationOlapConnectionStringJson, new DocumentInfo { Id = "", Document = sourceOlapConnectionStringJson }, changes: null);
                    Assert.False(changed);
                }

                operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                {
                    OperateOnDatabaseRecordTypes = DatabaseRecordItemType.Analyzers
                }, source.Smuggler.ForDatabase(destination2.Database));
                await operation.WaitForCompletionAsync();

                var destinationRecord2 = await source.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(destination2.Database));
                Assert.Equal(0, destinationRecord2.OlapConnectionStrings.Count);
                Assert.Equal(0, destinationRecord2.OlapEtls.Count);

                var exportPath = NewDataPath();

                operation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportPath);
                await operation.WaitForCompletionAsync();

                operation = await source.Smuggler.ForDatabase(destination3.Database).ImportAsync(new DatabaseSmugglerImportOptions { OperateOnDatabaseRecordTypes = DatabaseRecordItemType.Analyzers }, exportPath);
                await operation.WaitForCompletionAsync();

                var destinationRecord3 = await source.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(destination3.Database));
                Assert.Equal(0, destinationRecord3.OlapConnectionStrings.Count);
                Assert.Equal(0, destinationRecord3.OlapEtls.Count);
            }
        }

        private void SetupLocalOlapEtl(DocumentStore store, string script, string path)
        {
            var connectionStringName = $"{store.Database} to local";
            var configuration = new OlapEtlConfiguration
            {
                ConnectionStringName = connectionStringName,
                RunFrequency = "* * * * *",
                Transforms =
                {
                    new Transformation
                    {
                        Name = "MonthlyOrders",
                        Collections = new List<string> {"Orders"},
                        Script = script
                    }
                }
            };

            var connectionString = new OlapConnectionString
            {
                Name = connectionStringName,
                LocalSettings = new LocalSettings
                {
                    FolderPath = path
                }
            };

            AddEtl(store, configuration, connectionString);
        }
    }
}
