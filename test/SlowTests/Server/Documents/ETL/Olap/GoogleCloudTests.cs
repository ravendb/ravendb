using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FastTests.Client;
using FastTests.Server.Basic.Entities;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class GoogleCloudTests : EtlTestBase
    {
        public GoogleCloudTests(ITestOutputHelper output) : base(output)
        {
        }

        private readonly string _googleCloudTestsPrefix = $"olap-tests/{nameof(GoogleCloudTests)}-{Guid.NewGuid()}";
        private const string CollectionName = "Orders";

        [GoogleCloudFact]
        public async Task CanUploadToGoogleCloud()
        {
            var settings = GetGoogleCloudSettings();

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

loadToOrders(partitionBy(key),
    {
        Company : this.Company,
        ShipVia : this.ShipVia
    })
";
                    SetupGoogleCloudOlapEtl(store, script, settings);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenGoogleCloudClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";
                        var cloudObjects = await client.ListObjectsAsync(prefix);

                        Assert.Equal(2, cloudObjects.Count);
                        Assert.Contains("2020-01-01", cloudObjects[0].Name);
                        Assert.Contains("2020-02-01", cloudObjects[1].Name);
                    }
                }
            }

            finally
            {
                await DeleteObjects(settings);
            }
        }

        [GoogleCloudFact]
        public async Task SimpleTransformation()
        {
            var settings = GetGoogleCloudSettings();

            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1; i <= 10; i++)
                        {
                            var o = new Query.Order
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

loadToOrders(partitionBy(key),
    {
        Company : this.Company,
        ShipVia : this.ShipVia
    })
";
                    SetupGoogleCloudOlapEtl(store, script, settings);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenGoogleCloudClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";

                        var cloudObjects = await client.ListObjectsAsync(prefix);

                        Assert.Equal(1, cloudObjects.Count);

                        var fullPath = cloudObjects[0].Name;
                        var stream = client.DownloadObject(fullPath);
                        var ms = new MemoryStream();
                        stream.CopyTo(ms);

                        using (var parquetReader = new ParquetReader(ms))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);

                            var expectedFields = new[] { "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));

                                var data = rowGroupReader.ReadColumn((DataField)field).Data;
                                Assert.True(data.Length == 10);

                                if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                                    continue;

                                var count = 1;
                                foreach (var val in data)
                                {
                                    switch (field.Name)
                                    {
                                        case ParquetTransformedItems.DefaultIdColumn:
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

            finally
            {
                await DeleteObjects(settings);
            }
        }

        [GoogleCloudFact]
        public async Task CanLoadToMultipleTables()
        {
            const string salesTableName = "Sales";
            var settings = GetGoogleCloudSettings();

            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 31; i++)
                        {
                            var orderedAt = baseline.AddDays(i);
                            var lines = new List<OrderLine>();

                            for (int j = 1; j <= 5; j++)
                            {
                                lines.Add(new OrderLine
                                {
                                    Quantity = j * 10,
                                    PricePerUnit = i + j,
                                    Product = $"Products/{j}"
                                });
                            }

                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                Company = $"companies/{i}",
                                Lines = lines
                            };

                            await session.StoreAsync(o);
                        }

                        baseline = baseline.AddMonths(1);

                        for (int i = 0; i < 28; i++)
                        {
                            var orderedAt = baseline.AddDays(i);
                            var lines = new List<OrderLine>();

                            for (int j = 1; j <= 5; j++)
                            {
                                lines.Add(new OrderLine
                                {
                                    Quantity = j * 10,
                                    PricePerUnit = i + j,
                                    Product = $"Products/{j}"
                                });
                            }

                            var o = new Query.Order
                            {
                                Id = $"orders/{i + 31}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                Company = $"companies/{i}",
                                Lines = lines
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var orderData = {
    Company : this.Company,
    RequireAt : new Date(this.RequireAt),
    ItemsCount: this.Lines.length,
    TotalCost: 0
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    orderData.TotalCost += (line.PricePerUnit * line.Quantity);
    
    // load to 'sales' table

    loadToSales(partitionBy(key), {
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}

// load to 'orders' table
loadToOrders(partitionBy(key), orderData);
";

                    SetupGoogleCloudOlapEtl(store, script, settings);
                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenGoogleCloudClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";
                        var cloudObjects = await client.ListObjectsAsync(prefix);

                        Assert.Equal(2, cloudObjects.Count);
                        Assert.Contains("2020-01-01", cloudObjects[0].Name);
                        Assert.Contains("2020-02-01", cloudObjects[1].Name);

                        var fullPath = cloudObjects[0].Name;
                        var stream = client.DownloadObject(fullPath);
                        var ms = new MemoryStream();
                        await stream.CopyToAsync(ms);

                        using (var parquetReader = new ParquetReader(ms))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);

                            var expectedFields = new[] { "Company", "RequireAt", "ItemsCount", "TotalCost", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };
                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));

                                var data = rowGroupReader.ReadColumn((DataField)field).Data;
                                Assert.True(data.Length == 31);
                            }
                        }
                    }

                    using (var client = new RavenGoogleCloudClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{salesTableName}";
                        var cloudObjects = await client.ListObjectsAsync(prefix);

                        Assert.Equal(2, cloudObjects.Count);
                        Assert.Contains("2020-01-01", cloudObjects[0].Name);
                        Assert.Contains("2020-02-01", cloudObjects[1].Name);

                        var fullPath = cloudObjects[1].Name;
                        var stream = client.DownloadObject(fullPath);
                        var ms = new MemoryStream();
                        await stream.CopyToAsync(ms);

                        using (var parquetReader = new ParquetReader(ms))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);

                            var expectedFields = new[] { "Qty", "Product", "Cost", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };
                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));

                                var data = rowGroupReader.ReadColumn((DataField)field).Data;
                                Assert.True(data.Length == 28 * 5);
                            }
                        }
                    }
                }

            }
            finally
            {
                await DeleteObjects(settings);
            }
        }

        [GoogleCloudFact]
        public async Task CanModifyPartitionColumnName()
        {
            var settings = GetGoogleCloudSettings();

            try
            {
                using (var store = GetDocumentStore())
                {
                    const string partitionColumn = "order_date";

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

loadToOrders(partitionBy(['order_date', key]),
    {
        Company : this.Company,
        ShipVia : this.ShipVia
    })
";
                    var connectionStringName = $"{store.Database} to GCP";

                    var configuration = new OlapEtlConfiguration
                    {
                        Name = "olap-gcp-test",
                        ConnectionStringName = connectionStringName,
                        RunFrequency = LocalTests.DefaultFrequency,
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

                    SetupGoogleCloudOlapEtl(store, settings, configuration);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenGoogleCloudClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";
                        var cloudObjects = await client.ListObjectsAsync(prefix);

                        Assert.Equal(2, cloudObjects.Count);

                        Assert.Contains($"{partitionColumn}=2020-01-01", cloudObjects[0].Name);
                        Assert.Contains($"{partitionColumn}=2020-02-01", cloudObjects[1].Name);
                    }
                }
            }

            finally
            {
                await DeleteObjects(settings);
            }
        }

        [GoogleCloudFact]
        public async Task SimpleTransformation_NoPartition()
        {
            var settings = GetGoogleCloudSettings();
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1).ToUniversalTime();

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 100; i++)
                        {
                            await session.StoreAsync(new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = baseline.AddDays(i),
                                ShipVia = $"shippers/{i}",
                                Company = $"companies/{i}"
                            });
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
loadToOrders(noPartition(),
    {
        OrderDate : this.OrderedAt
        Company : this.Company,
        ShipVia : this.ShipVia
    });
";
                    SetupGoogleCloudOlapEtl(store, script, settings);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenGoogleCloudClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";

                        var cloudObjects = await client.ListObjectsAsync(prefix);

                        Assert.Equal(1, cloudObjects.Count);

                        var stream = client.DownloadObject(cloudObjects[0].Name);
                        var ms = new MemoryStream();
                        stream.CopyTo(ms);

                        using (var parquetReader = new ParquetReader(ms))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);

                            var expectedFields = new[] { "OrderDate", "ShipVia", "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));

                                var data = rowGroupReader.ReadColumn((DataField)field).Data;
                                Assert.True(data.Length == 100);

                                if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                                    continue;

                                var count = 0;
                                foreach (var val in data)
                                {
                                    if (field.Name == "OrderDate")
                                    {
                                        var expectedDto = new DateTimeOffset(DateTime.SpecifyKind(baseline.AddDays(count), DateTimeKind.Utc));
                                        Assert.Equal(expectedDto, val);
                                    }

                                    else
                                    {
                                        var expected = field.Name switch
                                        {
                                            ParquetTransformedItems.DefaultIdColumn => $"orders/{count}",
                                            "Company" => $"companies/{count}",
                                            "ShipVia" => $"shippers/{count}",
                                            _ => null
                                        };

                                        Assert.Equal(expected, val);
                                    }

                                    count++;
                                }
                            }
                        }
                    }

                }

            }
            finally
            {
                await DeleteObjects(settings);
            }
        }

        [GoogleCloudFact]
        public async Task SimpleTransformation_MultiplePartitions()
        {
            var settings = GetGoogleCloudSettings();
            try
            {
                using (var store = GetDocumentStore())
                {
                    var baseline = DateTime.SpecifyKind(new DateTime(2020, 1, 1), DateTimeKind.Utc);

                    using (var session = store.OpenAsyncSession())
                    {
                        const int total = 31 + 28; // days in January + days in February 

                        for (int i = 0; i < total; i++)
                        {
                            var orderedAt = baseline.AddDays(i);
                            await session.StoreAsync(new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                ShipVia = $"shippers/{i}",
                                Company = $"companies/{i}"
                            });
                        }

                        for (int i = 1; i <= 37; i++)
                        {
                            var index = i + total;
                            var orderedAt = baseline.AddYears(1).AddMonths(1).AddDays(i);
                            await session.StoreAsync(new Query.Order
                            {
                                Id = $"orders/{index}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                ShipVia = $"shippers/{index}",
                                Company = $"companies/{index}"
                            });
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var orderDate = new Date(this.OrderedAt);

loadToOrders(partitionBy(
    ['year', orderDate.getFullYear()],
    ['month', orderDate.getMonth() + 1]
),
    {
        Company : this.Company,
        ShipVia : this.ShipVia,
        RequireAt : this.RequireAt
    });
";
                    SetupGoogleCloudOlapEtl(store, script, settings);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var expectedFields = new[] { "RequireAt", "ShipVia", "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    using (var client = new RavenGoogleCloudClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}/";
                        var cloudObjects = await client.ListObjectsAsync(prefix);

                        Assert.Equal(4, cloudObjects.Count);
                        Assert.Contains("Orders/year=2020/month=1", cloudObjects[0].Name);
                        Assert.Contains("Orders/year=2020/month=2", cloudObjects[1].Name);
                        Assert.Contains("Orders/year=2021/month=2", cloudObjects[2].Name);
                        Assert.Contains("Orders/year=2021/month=3", cloudObjects[3].Name);
                    }
                }
            }
            finally
            {
                await DeleteObjects(settings);
            }
        }

        [GoogleCloudFact]
        public async Task CanPartitionByCustomDataFieldViaScript()
        {
            var settings = GetGoogleCloudSettings();
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
var month = orderDate.getMonth() + 1;

loadToOrders(partitionBy(['year', year], ['month', month], ['source', $customPartitionValue]),
{
    Company : this.Company,
    ShipVia : this.ShipVia
});
";

                    const string customPartition = "shop-16";
                    SetupGoogleCloudOlapEtl(store, script, settings, customPartition: customPartition);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenGoogleCloudClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}/";
                        var cloudObjects = await client.ListObjectsAsync(prefix);

                        Assert.Equal(2, cloudObjects.Count);
                        Assert.Contains($"/Orders/year=2020/month=1/source={customPartition}/", cloudObjects[0].Name);
                        Assert.Contains($"/Orders/year=2020/month=2/source={customPartition}/", cloudObjects[1].Name);
                    }
                }
            }
            finally
            {
                await DeleteObjects(settings);
            }
        }

        [GoogleCloudFact]
        public async Task CanHandleSpecialCharsInFolderPath()
        {
            var settings = GetGoogleCloudSettings();
            try
            {
                using (var store = GetDocumentStore())
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var today = DateTime.Today;
                        await session.StoreAsync(new Order
                        {
                            OrderedAt = today,
                            Lines = new List<OrderLine>
                            {
                                new OrderLine
                                {
                                    ProductName = "Wimmers gute Semmelknödel",
                                    PricePerUnit = 12
                                },
                                new OrderLine
                                {
                                    ProductName = "Guaraná Fantástica",
                                    PricePerUnit = 42
                                },

                                new OrderLine
                                {
                                    ProductName = "Thüringer Rostbratwurst",
                                    PricePerUnit = 19
                                }
                            }
                        });

                        await session.StoreAsync(new Order
                        {
                            OrderedAt = today.AddYears(-1),
                            Lines = new List<OrderLine>
                            {
                                new OrderLine
                                {
                                    ProductName = "Uncle Bob's Cajon Sauce",
                                    PricePerUnit = 11
                                },
                                new OrderLine
                                {
                                    ProductName = "Côte de Blaye",
                                    PricePerUnit = 25
                                }
                            }
                        });

                        await session.StoreAsync(new Order
                        {
                            OrderedAt = today.AddYears(-2),
                            Lines = new List<OrderLine>
                            {
                                new OrderLine
                                {
                                    ProductName = "גבינה צהובה",
                                    PricePerUnit = 20
                                },
                                new OrderLine
                                {
                                    ProductName = "במבה",
                                    PricePerUnit = 7
                                }
                            }
                        });

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
for (var i = 0; i < this.Lines.length; i++){
    var line  = this.Lines[i];
    loadToOrders(partitionBy(['product-name', line.ProductName]), {
        PricePerUnit: line.PricePerUnit,
        OrderedAt: this.OrderedAt
})}";
                    SetupGoogleCloudOlapEtl(store, script, settings);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenGoogleCloudClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";
                        var cloudObjects = await client.ListObjectsAsync(prefix);

                        Assert.Equal(7, cloudObjects.Count);

                        Assert.Contains("Côte de Blaye", cloudObjects[0].Name);
                        Assert.Contains("Guaraná Fantástica", cloudObjects[1].Name);
                        Assert.Contains("Thüringer Rostbratwurst", cloudObjects[2].Name);
                        Assert.Contains("Uncle Bob's Cajon Sauce", cloudObjects[3].Name);
                        Assert.Contains("Wimmers gute Semmelknödel", cloudObjects[4].Name);
                        Assert.Contains("במבה", cloudObjects[5].Name);
                        Assert.Contains("גבינה צהובה", cloudObjects[6].Name);
                    }
                }
            }
            finally
            {
                await DeleteObjects(settings);
            }
        }

        private void SetupGoogleCloudOlapEtl(DocumentStore store, string script, GoogleCloudSettings settings, string customPartition = null, string transformationName = null)
        {
            var connectionStringName = $"{store.Database} to GoogleCloud";

            var configuration = new OlapEtlConfiguration
            {
                Name = "olap-gcp-test",
                ConnectionStringName = connectionStringName,
                RunFrequency = LocalTests.DefaultFrequency,
                CustomPartitionValue = customPartition,
                Transforms =
                {
                    new Transformation
                    {
                        Name = transformationName ?? "MonthlyOrders",
                        Collections = new List<string> {"Orders"},
                        Script = script
                    }
                }
            };
            AddEtl(store, configuration, new OlapConnectionString
            {
                Name = connectionStringName,
                GoogleCloudSettings = settings
            });
        }

        private void SetupGoogleCloudOlapEtl(DocumentStore store, GoogleCloudSettings settings, OlapEtlConfiguration configuration)
        {
            var connectionStringName = $"{store.Database} to GCP";
            AddEtl(store, configuration, new OlapConnectionString
            {
                Name = connectionStringName,
                GoogleCloudSettings = settings
            });
        }

        private GoogleCloudSettings GetGoogleCloudSettings([CallerMemberName] string caller = null)
        {
            var googleCloudSettings = GoogleCloudFactAttribute.GoogleCloudSettings;
            if (googleCloudSettings == null)
                return null;

            var remoteFolderName = _googleCloudTestsPrefix;
            if (string.IsNullOrEmpty(caller) == false)
                remoteFolderName = $"{remoteFolderName}/{caller}";

            if (string.IsNullOrEmpty(googleCloudSettings.RemoteFolderName) == false)
                remoteFolderName = $"{googleCloudSettings.RemoteFolderName}/{remoteFolderName}";

            googleCloudSettings.RemoteFolderName = remoteFolderName;

            return googleCloudSettings;
        }

        private static async Task DeleteObjects(GoogleCloudSettings settings)
        {
            if (settings == null)
                return;

            try
            {
                using (var client = new RavenGoogleCloudClient(settings))
                {
                    var all = await client.ListObjectsAsync(prefix: settings.RemoteFolderName);
                    foreach (var obj in all)
                    {
                        await client.DeleteObjectAsync(obj.Name);
                    }
                }
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }
}
