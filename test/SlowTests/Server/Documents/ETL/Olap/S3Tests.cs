using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static FastTests.Client.Query;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class S3Tests : EtlTestBase
    {
        public S3Tests(ITestOutputHelper output) : base(output)
        {
        }

        private static string _s3TestPrefix = "olap";

        [AmazonS3Fact]
        public async Task CanUploadToS3()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 31; i++)
                    {
                        await session.StoreAsync(new Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            ShipVia = $"shippers/{i}",
                            Company = $"companies/{i}"
                        });
                    }

                    for (int i = 0; i < 28; i++)
                    {
                        await session.StoreAsync(new Order
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
    })
";
                var settings = GetS3Settings();
                SetupS3ParquetEtl(store, script, settings, TimeSpan.FromMinutes(10));

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var s3Client = new RavenAwsS3Client(settings))
                {
                    var cloudObjects = await s3Client.ListObjectsAsync(settings.RemoteFolderName, string.Empty, false);

                    Assert.Equal(2, cloudObjects.FileInfoDetails.Count);
                    Assert.Contains("2020-01-01", cloudObjects.FileInfoDetails[0].FullPath);
                    Assert.Contains("2020-02-01", cloudObjects.FileInfoDetails[1].FullPath);
                }
            }
        }


        [AmazonS3Fact]
        public async Task SimpleTransformation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var o = new Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            Company = $"companies/{i}"
                        };

                        await session.StoreAsync(o);
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
    })
";
                var settings = GetS3Settings();
                SetupS3ParquetEtl(store, script, settings, TimeSpan.FromMinutes(10));

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var s3Client = new RavenAwsS3Client(settings))
                {
                    var cloudObjects = await s3Client.ListObjectsAsync(settings.RemoteFolderName, string.Empty, false);

                    Assert.Equal(1, cloudObjects.FileInfoDetails.Count);

                    var blob = await s3Client.GetObjectAsync(cloudObjects.FileInfoDetails[0].FullPath);

                    await using var ms = new MemoryStream();
                    blob.Data.CopyTo(ms);

                    using (var parquetReader = new ParquetReader(ms))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);

                        var expectedFields = new[] { "OrderId", "Company" };

                        Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                        using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                        foreach (var field in parquetReader.Schema.Fields)
                        {
                            Assert.True(field.Name.In(expectedFields));

                            var data = rowGroupReader.ReadColumn((DataField)field).Data;
                            Assert.True(data.Length == 10);

                            var count = 1;
                            foreach (var val in data)
                            {
                                switch (field.Name)
                                {
                                    case "OrderId":
                                        Assert.Equal($"orders/{count}", val);
                                        break;
                                    case "Company":
                                        Assert.Equal($"companies/{count}", val);
                                        break;
                                }

                                count++;

                            }
                        }
                    }
                }
            }
        }

        protected void SetupS3ParquetEtl(DocumentStore store, string script, S3Settings settings, TimeSpan frequency)
        {
            var connectionStringName = $"{store.Database} to S3";

            AddEtl(store, new OlapEtlConfiguration
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                RunFrequency = frequency,
                Transforms =
                {
                    new Transformation
                    {
                        Name = "MonthlyOrders",
                        Collections = new List<string> {"Orders"},
                        Script = script
                    }
                }
            }, new OlapEtlConnectionString
            {
                Name = connectionStringName,
                S3Settings = settings
            });
        }

        private static S3Settings GetS3Settings(string subPath = null)
        {
            var s3Settings = AmazonS3FactAttribute.S3Settings;

            if (s3Settings == null)
                return null;

            var remoteFolderName = $"{s3Settings.RemoteFolderName}/{_s3TestPrefix}";

            if (string.IsNullOrEmpty(subPath) == false)
                remoteFolderName = $"{remoteFolderName}/{subPath}";

            return new S3Settings
            {
                BucketName = s3Settings.BucketName,
                RemoteFolderName = remoteFolderName,
                AwsAccessKey = s3Settings.AwsAccessKey,
                AwsSecretKey = s3Settings.AwsSecretKey,
                AwsRegionName = s3Settings.AwsRegionName
            };
        }

        public override void Dispose()
        {
            base.Dispose();

            var s3Settings = GetS3Settings();
            if (s3Settings == null)
                return;

            try
            {
                using (var s3Client = new RavenAwsS3Client(s3Settings))
                {
                    var cloudObjects = s3Client.ListObjectsAsync(s3Settings.RemoteFolderName, delimiter: string.Empty, listFolders: false).GetAwaiter().GetResult();
                    var pathsToDelete = cloudObjects.FileInfoDetails.Select(x => x.FullPath).ToList();

                    s3Client.DeleteMultipleObjects(pathsToDelete);
                }
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }
}
