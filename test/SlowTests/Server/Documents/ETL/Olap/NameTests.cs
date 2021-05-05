using System.Collections.Generic;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class NameTests : EtlTestBase
    {
        public NameTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void NameUniqueness()
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

            using (var store = GetDocumentStore())
            {
                SetupLocalOlapEtl(store, script, path);
                SetupLocalOlapEtl(store, script, path);

                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(2, record.OlapEtls.Count);

                var name1 = record.OlapEtls[0].Name;
                var name2 = record.OlapEtls[1].Name;

                Assert.NotEqual(name1, name2);
                Assert.Contains(name1, name2);
                Assert.Equal($"{name1} #2", name2);

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
