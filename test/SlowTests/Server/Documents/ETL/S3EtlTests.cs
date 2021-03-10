using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests.Client;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class S3EtlTests : EtlTestBase
    {
        public S3EtlTests(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task SimpleTransformation()
        {
            var path = GetTempPath("Orders");
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 31; i++)
                        {
                            await session.StoreAsync(new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = baseline.AddDays(i),
                                ShipVia = $"shippers/{i}",
                                Company = $"companies/{i}"
                            });
                        }

                        for (int i = 0; i < 28; i++)
                        {
                            await session.StoreAsync(new Query.Order
                            {
                                Id = $"orders/{i + 31}",
                                OrderedAt = baseline.AddMonths(1).AddDays(i),
                                ShipVia = $"shippers/{i + 31}",
                                Company = $"companies/{i + 31}"
                            });
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);


                    var script = @"
var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(key,
    {
        OrderId: id(this),
        Company : this.Company,
        ShipVia : this.ShipVia
    });
";
                    SetupS3Etl(store, script, path, TimeSpan.FromMinutes(13));

                    etlDone.Wait(TimeSpan.FromMinutes(65));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(2, files.Length);

                    var expectedFields = new[] { "OrderId", "ShipVia", "Company" };

                    foreach (var fileName in files)
                    {
                        using (var fs = File.OpenRead(fileName))
                        using (var parquetReader = new ParquetReader(fs))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);
                            Assert.Equal(3, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));
                                var data = rowGroupReader.ReadColumn((DataField)field).Data;

                                Assert.True(data.Length == 31 || data.Length == 28);
                                var count = data.Length == 31 ? 0 : 31;

                                foreach (var val in data)
                                {
                                    var expected = field.Name switch
                                    {
                                        "OrderId" => $"orders/{count}",
                                        "Company" => $"companies/{count}",
                                        "ShipVia" => $"shippers/{count}",
                                        _ => null
                                    };

                                    Assert.Equal(expected, val);
                                    count++;
                                }

                            }

                        }

                    }
                }

            }
            finally
            {
                var di = new DirectoryInfo(path);
                foreach (var file in di.EnumerateFiles())
                {
                    file.Delete();
                }

                di.Delete();
            }
        }

        private static string GetTempPath(string name)
        {
            var tmpPath = Path.GetTempPath();
            return Directory.CreateDirectory(Path.Combine(tmpPath, name)).FullName;
        }

        protected void SetupS3Etl(DocumentStore store, string script, string path, TimeSpan frequency)
        {
            var connectionStringName = $"{store.Database} to S3";

            AddEtl(store, new S3EtlConfiguration
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                TempDirectoryPath = path,
                ETLFrequency = frequency,
                Transforms =
                {
                    new Transformation
                    {
                        Name = "MonthlyOrders",
                        Collections = new List<string> {"Orders"},
                        Script = script
                    }
                }
            }, new S3ConnectionString
            {
                Name = connectionStringName,
                S3Settings = new S3Settings()
            });
        }
    }
}
