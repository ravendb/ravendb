using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class AzureTests : EtlTestBase
    {
        public AzureTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string AzureTestsPrefix = "olap/tests";
        private const string CollectionName = "Orders";

        [AzureFact]
        public async Task CanUploadToAzure()
        {
            var settings = GetAzureSettings();

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
                    SetupAzureEtl(store, script, settings);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenAzureClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";
                        var result = await client.ListBlobsAsync(prefix, delimiter: string.Empty, listFolders: false);
                        var list = result.List.ToList();

                        Assert.Equal(2, list.Count);
                        Assert.Contains("2020-01-01", list[0].Name);
                        Assert.Contains("2020-02-01", list[1].Name);
                    }
                }
            }

            finally
            {
                await DeleteObjects(settings);
            }
        }

        [AzureFact]
        public async Task SimpleTransformation()
        {
            var settings = GetAzureSettings();

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
                                Company = $"companies/{i}",
                                ShipVia = $"shippers/{i}"
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
                    SetupAzureEtl(store, script, settings);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenAzureClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";
                        var result = await client.ListBlobsAsync(prefix, delimiter: string.Empty, listFolders: false);
                        var list = result.List.ToList();
                        Assert.Equal(1, list.Count);

                        var blob = await client.GetBlobAsync(list[0].Name);
                        await using var ms = new MemoryStream();
                        blob.Data.CopyTo(ms);

                        using (var parquetReader = new ParquetReader(ms))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);

                            var expectedFields = new[] { "Company", "ShipVia", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

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
                                        case "ShipVia":
                                            Assert.Equal($"shippers/{count}", val);
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

        [AzureFact]
        public async Task CanLoadToMultipleTables()
        {
            const string salesTableName = "Sales";
            var settings = GetAzureSettings();

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


                    SetupAzureEtl(store, script, settings);
                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenAzureClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";
                        var result = await client.ListBlobsAsync(prefix, delimiter: string.Empty, listFolders: false);
                        var list = result.List.ToList();

                        Assert.Equal(2, list.Count);
                        Assert.Contains("2020-01-01", list[0].Name);
                        Assert.Contains("2020-02-01", list[1].Name);

                        var blob = await client.GetBlobAsync(list[0].Name);
                        await using var ms = new MemoryStream();
                        blob.Data.CopyTo(ms);

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

                    //sales
                    using (var client = new RavenAzureClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{salesTableName}";
                        var result = await client.ListBlobsAsync(prefix, delimiter: string.Empty, listFolders: false);
                        var list = result.List.ToList();

                        Assert.Equal(2, list.Count);
                        Assert.Contains("2020-01-01", list[0].Name);
                        Assert.Contains("2020-02-01", list[1].Name);

                        var blob = await client.GetBlobAsync(list[1].Name);
                        await using var ms = new MemoryStream();
                        blob.Data.CopyTo(ms);

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
                await DeleteObjects(settings, salesTableName);
            }
        }

        [AzureFact]
        public async Task CanModifyPartitionColumnName()
        {
            var settings = GetAzureSettings();

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
                    var connectionStringName = $"{store.Database} to Azure";

                    var configuration = new OlapEtlConfiguration
                    {
                        Name = "olap-azure-test",
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

                    SetupAzureEtl(store, settings, configuration);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenAzureClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";
                        var cloudObjects = await client.ListBlobsAsync(prefix, string.Empty, false);
                        var list = cloudObjects.List.ToList();
                        
                        Assert.Equal(2, list.Count);
                        Assert.Contains($"{partitionColumn}=2020-01-01", list[0].Name);
                        Assert.Contains($"{partitionColumn}=2020-02-01", list[1].Name);
                    }
                }
            }

            finally
            {
                await DeleteObjects(settings);
            }
        }

        [AzureFact]
        public async Task SimpleTransformation_NoPartition()
        {
            var settings = GetAzureSettings();
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
                    SetupAzureEtl(store, script, settings);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenAzureClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";

                        var cloudObjects = await client.ListBlobsAsync(prefix, delimiter: string.Empty, listFolders: false);
                        var list = cloudObjects.List.ToList();

                        Assert.Equal(1, list.Count);

                        var blob = await client.GetBlobAsync(list[0].Name);
                        await using var ms = new MemoryStream();
                        blob.Data.CopyTo(ms);

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

        [AzureFact]
        public async Task SimpleTransformation_MultiplePartitions()
        {
            var settings = GetAzureSettings();
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
                    SetupAzureEtl(store, script, settings);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var expectedFields = new[] { "RequireAt", "ShipVia", "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                    using (var client = new RavenAzureClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}/";
                        var cloudObjects = await client.ListBlobsAsync(prefix, delimiter: "/", listFolders: true);
                        var list = cloudObjects.List.ToList();

                        Assert.Equal(2, list.Count);
                        Assert.Contains("Orders/year=2020/", list[0].Name);
                        Assert.Contains("Orders/year=2021/", list[1].Name);

                        for (var index = 1; index <= list.Count; index++)
                        {
                            var folder = list[index - 1];
                            var objectsInFolder = await client.ListBlobsAsync(prefix: folder.Name, delimiter: "/", listFolders: true);
                            var objects = objectsInFolder.List.ToList();
                            Assert.Equal(2, objects.Count);
                            Assert.Contains($"month={index}/", objects[0].Name);
                            Assert.Contains($"month={index + 1}/", objects[1].Name);
                        }

                        var files = await ListAllFilesInFolders(client, list);
                        Assert.Equal(4, files.Count);

                        foreach (var filePath in files)
                        {
                            var blob = await client.GetBlobAsync(filePath);
                            await using var ms = new MemoryStream();
                            blob.Data.CopyTo(ms);

                            using (var parquetReader = new ParquetReader(ms))
                            {
                                Assert.Equal(1, parquetReader.RowGroupCount);
                                Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                                using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                                foreach (var field in parquetReader.Schema.Fields)
                                {
                                    Assert.True(field.Name.In(expectedFields));
                                    var data = rowGroupReader.ReadColumn((DataField)field).Data;

                                    Assert.True(data.Length == 31 || data.Length == 28 || data.Length == 27 || data.Length == 10);
                                    if (field.Name != "RequireAt")
                                        continue;

                                    var count = data.Length switch
                                    {
                                        31 => 0,
                                        28 => 31,
                                        27 => 365 + 33,
                                        10 => 365 + 33 + 27,
                                        _ => throw new ArgumentOutOfRangeException()
                                    };

                                    foreach (var val in data)
                                    {
                                        var expectedOrderDate = new DateTimeOffset(DateTime.SpecifyKind(baseline.AddDays(count++), DateTimeKind.Utc));
                                        var expected = expectedOrderDate.AddDays(7);
                                        Assert.Equal(expected, val);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            finally
            {
                await DeleteObjects(settings, prefix: $"{settings.RemoteFolderName}/{CollectionName}/", delimiter: "/", listFolder: true);
            }
        }

        [AzureFact]
        public async Task CanUseCustomPrefix()
        {
            var settings = GetAzureSettings();
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
                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
                    SetupAzureEtl(store, script, settings, customPartition);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var client = new RavenAzureClient(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{CollectionName}";
                        var cloudObjects = await client.ListBlobsAsync(prefix, delimiter: string.Empty, listFolders: false);
                        var list = cloudObjects.List.ToList();

                        Assert.Equal(2, list.Count);
                        Assert.Contains($"/Orders/year=2020/month=1/source={customPartition}/", list[0].Name);
                        Assert.Contains($"/Orders/year=2020/month=2/source={customPartition}/", list[1].Name);
                    }
                }
            }

            finally
            {
                await DeleteObjects(settings, prefix: $"{settings.RemoteFolderName}/{CollectionName}", delimiter: string.Empty);
            }
        }

        private void SetupAzureEtl(DocumentStore store, string script, AzureSettings settings, string customPartition = null)
        {
            var connectionStringName = $"{store.Database} to Azure";

            var configuration = new OlapEtlConfiguration
            {
                Name = "olap-azure-test",
                ConnectionStringName = connectionStringName,
                RunFrequency = LocalTests.DefaultFrequency,
                CustomPartitionValue = customPartition,
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
            AddEtl(store, configuration, new OlapConnectionString
            {
                Name = connectionStringName,
                AzureSettings = settings
            });
        }

        private void SetupAzureEtl(DocumentStore store, AzureSettings settings, OlapEtlConfiguration configuration)
        {
            var connectionStringName = $"{store.Database} to Azure";
            AddEtl(store, configuration, new OlapConnectionString
            {
                Name = connectionStringName,
                AzureSettings = settings
            });
        }

        private static AzureSettings GetAzureSettings([CallerMemberName] string caller = null)
        {
            var settings = AzureFactAttribute.AzureSettings;
            if (settings == null)
                return null;

            var remoteFolderName = AzureTestsPrefix;
            if (string.IsNullOrEmpty(caller) == false)
                remoteFolderName = $"{remoteFolderName}/{caller}";

            if (string.IsNullOrEmpty(settings.RemoteFolderName) == false)
                remoteFolderName = $"{settings.RemoteFolderName}/{remoteFolderName}";

            return new AzureSettings
            {
                RemoteFolderName = remoteFolderName,
                AccountName = settings.AccountName,
                StorageContainer = settings.StorageContainer,
                AccountKey = settings.AccountKey,
                SasToken = settings.SasToken
            };
        }

        private static async Task DeleteObjects(AzureSettings azureSettings, string additionalTable = null)
        {
            if (azureSettings == null)
                return;

            await DeleteObjects(azureSettings, prefix: $"{azureSettings.RemoteFolderName}/{CollectionName}", delimiter: string.Empty);

            if (additionalTable == null)
                return;

            await DeleteObjects(azureSettings, prefix: $"{azureSettings.RemoteFolderName}/{additionalTable}", delimiter: string.Empty);
        }

        private static async Task DeleteObjects(AzureSettings azureSettings, string prefix, string delimiter, bool listFolder = false)
        {
            if (azureSettings == null)
                return;

            try
            {
                using (var client = new RavenAzureClient(azureSettings))
                {
                    var result = await client.ListBlobsAsync(prefix, delimiter, listFolder);
                    List<string> filesToDelete;

                    if (listFolder == false)
                    {
                        filesToDelete = result.List.Select(b => b.Name).ToList();
                    }
                    else
                    {
                        filesToDelete = await ListAllFilesInFolders(client, result.List);
                    }

                    if (filesToDelete.Count == 0)
                        return;

                    client.DeleteBlobs(filesToDelete);
                }
            }
            catch (Exception)
            {
                // ignored
            }
        }

        private static async Task<List<string>> ListAllFilesInFolders(RavenAzureClient client, IEnumerable<RavenStorageClient.BlobProperties> folders)
        {
            var files = new List<string>();
            foreach (var folder in folders)
            {
                var objectsInFolder = await client.ListBlobsAsync(prefix: folder.Name, delimiter: string.Empty, listFolders: false);
                files.AddRange(objectsInFolder.List.Select(b => b.Name));
            }

            return files;
        }
    }
}
