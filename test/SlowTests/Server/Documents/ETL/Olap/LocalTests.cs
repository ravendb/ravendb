using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Client;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class LocalTests : EtlTestBase
    {
        internal const string DefaultFrequency = "* * * * *"; // every minute
        private const string AllFilesPattern = "*.*";


        public LocalTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public Task SimpleTransformation()
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
            return SimpleTransformationInternal(script);
        }

        [Fact]
        public Task SimpleTransformation_DifferentLoadTo_Syntax1()
        {
            var script = @"
var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadTo('Orders', partitionBy(key),
    {
        Company : this.Company,
        ShipVia : this.ShipVia
    });
";

            return SimpleTransformationInternal(script);
        }

        [Fact]
        public Task SimpleTransformation_DifferentLoadTo_Syntax2()
        {
            var script = @"
var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadTo(""Orders"", partitionBy(key),
    {
        Company : this.Company,
        ShipVia : this.ShipVia
    });
";

            return SimpleTransformationInternal(script);
        }

        private async Task SimpleTransformationInternal(string script)
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    const int numberOfDaysInJanuary = 31;
                    const int numberOfDaysInFebruary = 28;

                    for (int i = 0; i < numberOfDaysInJanuary; i++)
                    {
                        await session.StoreAsync(new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            ShipVia = $"shippers/{i}",
                            Company = $"companies/{i}"
                        });
                    }

                    for (int i = 0; i < numberOfDaysInFebruary; i++)
                    {
                        var next = i + numberOfDaysInJanuary;
                        await session.StoreAsync(new Query.Order
                        {
                            Id = $"orders/{next}",
                            OrderedAt = baseline.AddMonths(1).AddDays(i),
                            ShipVia = $"shippers/{next}",
                            Company = $"companies/{next}"
                        });
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(2, files.Length);

                var expectedFields = new[] { "ShipVia", "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                foreach (var fileName in files)
                {
                    using (var fs = File.OpenRead(fileName))
                    using (var parquetReader = new ParquetReader(fs))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);
                        Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                        using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                        foreach (var field in parquetReader.Schema.Fields)
                        {
                            Assert.True(field.Name.In(expectedFields));
                            var data = rowGroupReader.ReadColumn((DataField)field).Data;

                            Assert.True(data.Length == 31 || data.Length == 28);
                            var count = data.Length == 31 ? 0 : 31;

                            if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                                continue;

                            foreach (var val in data)
                            {
                                var expected = field.Name switch
                                {
                                    ParquetTransformedItems.DefaultIdColumn => $"orders/{count}",
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

        [Fact]
        public async Task SimpleTransformation2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var orderedAt = baseline.AddDays(i);
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = orderedAt,
                            RequireAt = orderedAt.AddDays(7),
                            Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = (decimal)1.25,
                                    }
                                }
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                var expectedFields = new[] { "RequireAt", "Total", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        Assert.True(field.Name.In(expectedFields));

                        var data = rowGroupReader.ReadColumn((DataField)field).Data;
                        Assert.True(data.Length == 10);

                        if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                            continue;

                        long count = 1;
                        foreach (var val in data)
                        {
                            switch (field.Name)
                            {
                                case ParquetTransformedItems.DefaultIdColumn:
                                    Assert.Equal($"orders/{count}", val);
                                    break;
                                case "RequireAt":
                                    var expected = new DateTimeOffset(DateTime.SpecifyKind(baseline.AddDays(count).AddDays(7), DateTimeKind.Utc));
                                    Assert.Equal(expected, val);
                                    break;
                                case "Total":
                                    var expectedTotal = count * 1.25M * 10;
                                    Assert.Equal(expectedTotal, val);
                                    break;
                            }

                            count++;

                        }
                    }
                }
            }
        }

        [Fact]
        public async Task SimpleTransformation_PartitionByDay()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < TimeSpan.FromDays(7).TotalHours; i++)
                    {
                        var orderedAt = baseline.AddHours(i);
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = orderedAt,
                            RequireAt = orderedAt.AddDays(7),
                            Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var day = orderDate.getDay();
var key = new Date(year, month, day);

loadToOrders(partitionBy(key), o);
";

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(7, files.Length);

                var expectedFields = new[] { "RequireAt", "Total", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        Assert.True(field.Name.In(expectedFields));

                        var data = rowGroupReader.ReadColumn((DataField)field).Data;
                        Assert.True(data.Length == 24);
                    }
                }
            }
        }

        [Fact]
        public async Task SimpleTransformation_PartitionByHour()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < TimeSpan.FromDays(1).TotalMinutes; i++)
                    {
                        var orderedAt = baseline.AddMinutes(i);
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = orderedAt,
                            RequireAt = orderedAt.AddDays(7),
                            Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var day = orderDate.getDay();
var hour = orderDate.getHours();
var key = new Date(year, month, day, hour);

loadToOrders(partitionBy(key), o);
";

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(24, files.Length);

                var expectedFields = new[] { "RequireAt", "Total", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                foreach (var file in files)
                {
                    using (var fs = File.OpenRead(file))
                    using (var parquetReader = new ParquetReader(fs))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);
                        Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                        using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                        foreach (var field in parquetReader.Schema.Fields)
                        {
                            Assert.True(field.Name.In(expectedFields));

                            var data = rowGroupReader.ReadColumn((DataField)field).Data;
                            Assert.True(data.Length == 60);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CanHandleMissingFieldsOnSomeDocuments()
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

                        if (i % 2 == 0)
                            o.Freight = i;

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var o = {
    Company : this.Company
};

if (this.Freight > 0)
    o.Freight = this.Freight

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                var expectedFields = new[] { "Company", "Freight", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
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
                                case "Freight":
                                    long expected = count % 2 == 0 ? count : 0;
                                    Assert.Equal(expected, val);
                                    break;
                            }

                            count++;

                        }
                    }
                }
            }

        }

        [Fact]
        public async Task CanHandleNullFieldValuesOnSomeDocument()
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
                        };

                        if (i % 2 == 0)
                            o.Company = "companies/" + i;

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var o = {
    Company : this.Company
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                var expectedFields = new[] { "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
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
                                    string expected = count % 2 == 0 ? $"companies/{count}" : null;
                                    Assert.Equal(expected, val);
                                    break;
                            }

                            count++;

                        }
                    }
                }
            }

        }

        [Fact]
        public async Task CanUseSettingFromScript()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var orderedAt = baseline.AddDays(i);
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = orderedAt,
                            RequireAt = orderedAt.AddDays(7),
                            Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var transformationScript = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                var connectionStringName = $"{store.Database} to S3";
                var path = NewDataPath(forceCreateDir: true);
                var scriptPath = GenerateConfigurationScript(path, out string command);
                var connectionString = new OlapConnectionString
                {
                    Name = connectionStringName,
                    LocalSettings = new LocalSettings
                    {
                        GetBackupConfigurationScript = new GetBackupConfigurationScript
                        {
                            Exec = command,
                            Arguments = scriptPath
                        }
                    }
                };

                var configuration = new OlapEtlConfiguration
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    RunFrequency = DefaultFrequency,
                    Transforms =
                        {
                            new Transformation
                            {
                                Name = "MonthlyOrders",
                                Collections = new List<string> {"Orders"},
                                Script = transformationScript
                            }
                        }
                };

                AddEtl(store, configuration, connectionString);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);
            }
        }

        [Fact]
        public async Task LastModifiedTicksShouldMatch()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                var ids = new List<string>();

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var orderedAt = baseline.AddDays(i);
                        var id = $"orders/{i}";
                        ids.Add(id);
                        var o = new Query.Order
                        {
                            Id = id,
                            OrderedAt = orderedAt,
                            RequireAt = orderedAt.AddDays(7),
                            Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";


                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                var expectedFields = new[] { "RequireAt", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var session = store.OpenAsyncSession())
                {
                    var docs = await session.LoadAsync<Order>(ids.ToArray());
                    string[] idFieldData = null;
                    long?[] lsatModifiedFieldData = null;

                    using (var fs = File.OpenRead(files[0]))
                    using (var parquetReader = new ParquetReader(fs))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);
                        Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                        using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                        foreach (var field in parquetReader.Schema.Fields)
                        {
                            Assert.True(field.Name.In(expectedFields));

                            var data = rowGroupReader.ReadColumn((DataField)field).Data;
                            Assert.True(data.Length == 10);

                            switch (field.Name)
                            {
                                case ParquetTransformedItems.LastModifiedColumn:
                                    lsatModifiedFieldData = (long?[])data;
                                    break;
                                case ParquetTransformedItems.DefaultIdColumn:
                                    idFieldData = (string[])data;
                                    break;
                                case "RequireAt":
                                    continue;
                            }
                        }
                    }

                    Assert.NotNull(idFieldData);
                    Assert.NotNull(lsatModifiedFieldData);

                    for (var index = 0; index < idFieldData.Length; index++)
                    {
                        var id = idFieldData[index];
                        Assert.True(docs.TryGetValue(id, out var doc));

                        var lastModifiedDateTime = session.Advanced.GetLastModifiedFor(doc);
                        Assert.True(lastModifiedDateTime.HasValue);

                        var expected = ParquetTransformedItems.UnixTimestampFromDateTime(lastModifiedDateTime.Value);
                        Assert.Equal(expected, lsatModifiedFieldData[index]);
                    }
                }

            }
        }

        [Fact]
        public async Task CanModifyIdColumnName()
        {
            using (var store = GetDocumentStore())
            {
                const string idColumn = "OrderId";

                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var orderedAt = baseline.AddDays(i);
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = orderedAt,
                            RequireAt = orderedAt.AddDays(7),
                            Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        Quantity = i * 10,
                                        PricePerUnit = i
                                    }
                                }
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                var connectionStringName = $"{store.Database} to local";
                var configuration = new OlapEtlConfiguration
                {
                    Name = "olap-test",
                    ConnectionStringName = connectionStringName,
                    RunFrequency = DefaultFrequency,
                    Transforms =
                        {
                            new Transformation
                            {
                                Name = "MonthlyOrders",
                                Collections = new List<string> {"Orders"},
                                Script = script
                            }
                        },
                    OlapTables = new List<OlapEtlTable>()
                        {
                            new OlapEtlTable
                            {
                                TableName = "Orders",
                                DocumentIdColumn = idColumn
                            }
                        }
                };


                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, configuration, path, connectionStringName);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                var expectedFields = new[] { "RequireAt", "Total", idColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        Assert.True(field.Name.In(expectedFields));

                        var data = rowGroupReader.ReadColumn((DataField)field).Data;
                        Assert.True(data.Length == 10);

                        if (field.Name == ParquetTransformedItems.LastModifiedColumn)
                            continue;

                        long count = 1;
                        foreach (var val in data)
                        {
                            switch (field.Name)
                            {
                                case idColumn:
                                    Assert.Equal($"orders/{count}", val);
                                    break;
                                case "RequireAt":
                                    var expected = new DateTimeOffset(DateTime.SpecifyKind(baseline.AddDays(count).AddDays(7), DateTimeKind.Utc));
                                    Assert.Equal(expected, val);
                                    break;
                                case "Total":
                                    var expectedTotal = count * count * 10;
                                    Assert.Equal(expectedTotal, val);
                                    break;
                            }

                            count++;

                        }
                    }
                }
            }
        }

        [Fact]
        public async Task SimpleTransformation_NoPartition()
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
                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                var expectedFields = new[] { "OrderDate", "ShipVia", "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                foreach (var fileName in files)
                {
                    using (var fs = File.OpenRead(fileName))
                    using (var parquetReader = new ParquetReader(fs))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);
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

        [Fact]
        public async Task SimpleTransformation_MultiplePartitions()
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
    ['month', orderDate.getMonth()]
),  
    {
        Company : this.Company,
        ShipVia : this.ShipVia,
        RequireAt : this.RequireAt
    });
";

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(4, files.Length);

                var expectedFields = new[] { "RequireAt", "ShipVia", "Company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                foreach (var fileName in files)
                {
                    using (var fs = File.OpenRead(fileName))
                    using (var parquetReader = new ParquetReader(fs))
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

        [Fact]
        public async Task CanHandleLazyNumbersWithTypeChanges()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        // after running the script, this is recognized as Int64
                        Double = 1.0
                    });

                    await session.StoreAsync(new User
                    {
                        // recognized as Decimal
                        Double = 2.22
                    });

                    await session.StoreAsync(new User
                    {
                        // recognized as Double
                        Double = double.MaxValue
                    });

                    await session.StoreAsync(new User
                    {
                        // recognized as Decimal
                        Double = 1.012345
                    });

                    await session.StoreAsync(new User
                    {
                        // recognized as Int64
                        Double = 6
                    });

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
    loadToUsers(noPartition(), {
        double : this.Double
    });";

                var connectionStringName = $"{store.Database} to local";
                var configuration = new OlapEtlConfiguration
                {
                    Name = "olap-test",
                    ConnectionStringName = connectionStringName,
                    RunFrequency = DefaultFrequency,
                    Transforms =
                        {
                            new Transformation
                            {
                                Name = "MonthlySales",
                                Collections = new List<string> {"Users"},
                                Script = script
                            }
                        }
                };

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, configuration, path, connectionStringName);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                var expectedFields = new[] { "double", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        Assert.True(field.Name.In(expectedFields));
                        var data = rowGroupReader.ReadColumn((DataField)field).Data;

                        Assert.True(data.Length == 5);

                        if (field.Name != "double")
                            continue;

                        var count = 0;
                        var expectedValues = new[]
                        {
                                1.0, 2.22, double.MaxValue, 1.012345, 6
                            };

                        foreach (var val in data)
                        {
                            Assert.Equal(expectedValues[count++], val);
                        }
                    }
                }
            }

        }

        [Fact]
        public async Task SimpleTransformation_CanUseSampleData()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                WaitForIndexing(store);

                long expectedCount;
                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Order>()
                        .GroupBy(o => new
                        {
                            o.OrderedAt.Year,
                            o.OrderedAt.Month
                        });

                    expectedCount = await query.CountAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    
    // load to 'sales' table

    loadToSales(partitionBy(key), {
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}";

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(expectedCount, files.Length);
            }
        }

        [Fact]
        public async Task CanSpecifyColumnTypeInScript()
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
                                    ProductName = "Cheese",
                                    PricePerUnit = 18,
                                    Quantity = 1
                                },
                                new OrderLine
                                {
                                    ProductName = "Eggs",
                                    PricePerUnit = 12.75M,
                                    Quantity = 2
                                },
                                new OrderLine
                                {
                                    ProductName = "Chicken",
                                    PricePerUnit = 42.99M,
                                    Quantity = 2
                                }
                            }
                    });

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    
    // load to 'sales' table

    loadToSales(noPartition(), {
        Quantity: line.Quantity,
        Product: line.ProductName,
        Cost: { Value: line.PricePerUnit, Type: 'Double' }
    });
}";
                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                var expectedFields = new[] { "Quantity", "Product", "Cost", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        Assert.True(field.Name.In(expectedFields));

                        var dataField = (DataField)field;
                        var data = rowGroupReader.ReadColumn(dataField).Data;

                        Assert.True(data.Length == 3);

                        if (field.Name != "Cost")
                            continue;

                        Assert.Equal(DataType.Double, dataField.DataType);

                        var count = 0;
                        var expectedValues = new[]
                        {
                                18, 12.75, 42.99
                            };

                        foreach (var val in data)
                        {
                            Assert.Equal(expectedValues[count++], val);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CanSpecifyColumnTypeInScript2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Byte = 1,
                        Decimal = 2.02M,
                        Double = 3.033,
                        Float = 4.0444F,
                        Int16 = 5,
                        Int32 = 6,
                        Int64 = 7L,
                        SByte = 8,
                        UInt16 = 9,
                        UInt32 = 10,
                        UInt64 = 11L
                    });

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
loadToUsers(noPartition(), {
    Byte: { Value: this.Byte, Type: 'Byte' },
    Decimal: { Value: this.Decimal, Type: 'Decimal' },
    Double: { Value: this.Double, Type: 'Double' },
    Float: { Value: this.Float, Type: 'Single' },
    Int16: { Value: this.Int16, Type: 'Int16' }, 
    Int32: { Value: this.Int32, Type: 'Int32' },
    Int64: { Value: this.Int64, Type: 'Int64' },
    SByte: { Value: this.SByte, Type: 'SByte' },
    UInt16: { Value: this.UInt16, Type: 'UInt16' },
    UInt32: { Value: this.UInt32, Type: 'UInt32' },
    UInt64: { Value: this.UInt64, Type: 'UInt64' }
});
";
                var connectionStringName = $"{store.Database} to local";
                var configuration = new OlapEtlConfiguration
                {
                    Name = "olap-test",
                    ConnectionStringName = connectionStringName,
                    RunFrequency = DefaultFrequency,
                    Transforms =
                        {
                            new Transformation
                            {
                                Name = "UsersData",
                                Collections = new List<string> {"Users"},
                                Script = script
                            }
                        }
                };

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, configuration, path, connectionStringName);
                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(1, files.Length);

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(13, parquetReader.Schema.Fields.Count);

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        var dataField = (DataField)field;
                        var data = rowGroupReader.ReadColumn(dataField).Data;
                        Assert.Equal(1, data.Length);

                        object expected = default;
                        switch (field.Name)
                        {
                            case nameof(User.Byte):
                                Assert.Equal(DataType.Byte, dataField.DataType);
                                expected = (byte)1;
                                break;
                            case nameof(User.Decimal):
                                Assert.Equal(DataType.Decimal, dataField.DataType);
                                expected = 2.02M;
                                break;
                            case nameof(User.Double):
                                Assert.Equal(DataType.Double, dataField.DataType);
                                expected = 3.033;
                                break;
                            case nameof(User.Float):
                                Assert.Equal(DataType.Float, dataField.DataType);
                                expected = 4.0444F;
                                break;
                            case nameof(User.Int16):
                                Assert.Equal(DataType.Short, dataField.DataType);
                                expected = (short)5;
                                break;
                            case nameof(User.Int32):
                                Assert.Equal(DataType.Int32, dataField.DataType);
                                expected = 6;
                                break;
                            case nameof(User.Int64):
                                Assert.Equal(DataType.Int64, dataField.DataType);
                                expected = 7L;
                                break;
                            case nameof(User.SByte):
                                Assert.Equal(DataType.SignedByte, dataField.DataType);
                                expected = (sbyte)8;
                                break;
                            case nameof(User.UInt16):
                                Assert.Equal(DataType.UnsignedInt16, dataField.DataType);
                                expected = (ushort)9;
                                break;
                            case nameof(User.UInt32):
                                Assert.Equal(DataType.UnsignedInt32, dataField.DataType);
                                expected = (uint)10;
                                break;
                            case nameof(User.UInt64):
                                Assert.Equal(DataType.UnsignedInt64, dataField.DataType);
                                expected = (ulong)11;
                                break;
                            case ParquetTransformedItems.DefaultIdColumn:
                            case ParquetTransformedItems.LastModifiedColumn:
                                continue;
                        }

                        foreach (var value in data)
                        {
                            Assert.Equal(expected, value);
                        }
                    }
                }
            }

        }

        [Fact]
        public async Task LocalOlapShouldCreateSubFoldersAccordingToPartition()
        {
            var countries = new[] { "Argentina", "Brazil", "Israel", "Poland", "United States" };

            using (var store = GetDocumentStore())
            {
                var dt = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            Company = $"companies/{i}",
                            Employee = $"employees/{i}",
                            OrderedAt = dt,
                            ShipTo = new Address
                            {
                                Country = countries[i % 5]
                            }
                        };

                        await session.StoreAsync(o);

                        dt = dt.AddDays(15);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var orderDate = new Date(this.OrderedAt);

loadToOrders(partitionBy(['year', orderDate.getFullYear()], ['month', orderDate.getMonth() + 1], ['country', this.ShipTo.Country]), 
{
    company: this.Company,
    employee: this.Employee
}
);
";

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                string[] files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories)
                    .OrderBy(x => x)
                    .ToArray();

                Assert.Equal(10, files.Length);

                var dirs = Directory.EnumerateDirectories(path).ToList();
                Assert.Equal(1, dirs.Count);
                var di = new DirectoryInfo(dirs[0]);
                Assert.Equal("Orders", di.Name);

                var subDirs = Directory.EnumerateDirectories(dirs[0]).ToList();
                Assert.Equal(1, subDirs.Count);
                di = new DirectoryInfo(subDirs[0]);
                Assert.Equal("year=2020", di.Name);

                var monthSubDirs = Directory.EnumerateDirectories(subDirs[0])
                    .OrderBy(x => x)
                    .ToList();

                Assert.Equal(5, monthSubDirs.Count);

                for (var index = 0; index < monthSubDirs.Count; index++)
                {
                    var month = index + 1;
                    var monthSubDir = monthSubDirs[index];
                    di = new DirectoryInfo(monthSubDir);
                    Assert.Equal($"month={month}", di.Name);

                    List<string> countryDirs;

                    switch (month)
                    {
                        case 1:
                            countryDirs = Directory.EnumerateDirectories(monthSubDir)
                                .OrderBy(x => x)
                                .ToList();

                            Assert.Equal(3, countryDirs.Count);
                            Assert.Contains("country=Brazil", countryDirs[0]);
                            Assert.Contains("country=Israel", countryDirs[1]);
                            Assert.Contains("country=Poland", countryDirs[2]);
                            break;
                        case 2:
                            countryDirs = Directory.EnumerateDirectories(monthSubDir)
                                .OrderBy(x => x)
                                .ToList();

                            Assert.Equal(1, countryDirs.Count);
                            Assert.Contains("country=United States", countryDirs[0]);
                            break;
                        case 3:
                            countryDirs = Directory.EnumerateDirectories(monthSubDir)
                                .OrderBy(x => x)
                                .ToList();

                            Assert.Equal(3, countryDirs.Count);
                            Assert.Contains("country=Argentina", countryDirs[0]);
                            Assert.Contains("country=Brazil", countryDirs[1]);
                            Assert.Contains("country=Israel", countryDirs[2]);
                            break;
                        case 4:
                            countryDirs = Directory.EnumerateDirectories(monthSubDir)
                                .OrderBy(x => x)
                                .ToList();

                            Assert.Equal(2, countryDirs.Count);
                            Assert.Contains("country=Poland", countryDirs[0]);
                            Assert.Contains("country=United States", countryDirs[1]);
                            break;
                        case 5:
                            countryDirs = Directory.EnumerateDirectories(monthSubDir)
                                .OrderBy(x => x)
                                .ToList();

                            Assert.Equal(1, countryDirs.Count);
                            Assert.Contains("country=Argentina", countryDirs[0]);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    foreach (var dir in countryDirs)
                    {
                        var parquets = Directory.GetFiles(dir);
                        Assert.Equal(1, parquets.Length);
                        Assert.EndsWith(".parquet", parquets[0]);
                    }
                }
            }
        }

        [Fact]
        public async Task ShouldCreateLocalFolderIfNotExists()
        {
            // RavenDB-16663

            using (var store = GetDocumentStore())
            {
                var dt = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 5; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            Company = $"companies/{i}",
                            Employee = $"employees/{i}",
                            OrderedAt = dt,
                        };

                        await session.StoreAsync(o);

                        dt = dt.AddYears(1);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var orderDate = new Date(this.OrderedAt);

loadToOrders(partitionBy(['year', orderDate.getFullYear()]), 
{
    company: this.Company,
    employee: this.Employee
}
);
";

                var path = NewDataPath(forceCreateDir: false);
                path = Path.Combine(path, "Aviv");

                Assert.False(Directory.Exists(path));

                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                string[] files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories)
                    .OrderBy(x => x)
                    .ToArray();

                Assert.Equal(5, files.Length);

                Assert.Contains("year=2020", files[0]);
                Assert.Contains("year=2021", files[1]);
                Assert.Contains("year=2022", files[2]);
                Assert.Contains("year=2023", files[3]);
                Assert.Contains("year=2024", files[4]);

            }
        }

        [Fact]
        public async Task CanUpdateDocIdColumnName()
        {
            using (var store = GetDocumentStore())
            {
                var dt = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 5; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            Company = $"companies/{i}",
                            Employee = $"employees/{i}",
                            OrderedAt = dt,
                        };

                        await session.StoreAsync(o);

                        dt = dt.AddYears(1);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var orderDate = new Date(this.OrderedAt);

loadToOrders(partitionBy(['year', orderDate.getFullYear()]), 
{
    company: this.Company,
    employee: this.Employee
}
);
";

                var path = NewDataPath(forceCreateDir: true);

                var connectionStringName = $"{store.Database} to local";
                var configuration = new OlapEtlConfiguration
                {
                    Name = "olap-test",
                    ConnectionStringName = connectionStringName,
                    RunFrequency = DefaultFrequency,
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

                var result = AddEtl(store, configuration, connectionString);
                var taskId = result.TaskId;

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories).OrderBy(x => x).ToList();

                Assert.Equal(5, files.Count);

                Assert.Contains("year=2020", files[0]);
                Assert.Contains("year=2021", files[1]);
                Assert.Contains("year=2022", files[2]);
                Assert.Contains("year=2023", files[3]);
                Assert.Contains("year=2024", files[4]);

                // update 

                const string documentIdColumn = "order_id";

                configuration.OlapTables = new List<OlapEtlTable>
                {
                    new OlapEtlTable
                    {
                        TableName = "Orders",
                        DocumentIdColumn = documentIdColumn
                    }
                };

                store.Maintenance.Send(new UpdateEtlOperation<OlapConnectionString>(taskId, configuration));

                // add more data

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 6; i <= 10; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            Company = $"companies/{i}",
                            Employee = $"employees/{i}",
                            OrderedAt = dt,
                        };

                        await session.StoreAsync(o);

                        dt = dt.AddYears(1);
                    }

                    await session.SaveChangesAsync();
                }

                etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
                etlDone.Wait(TimeSpan.FromMinutes(1));

                files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories).OrderBy(x => x).ToList();
                Assert.Equal(10, files.Count);

                Assert.Contains("year=2025", files[5]);
                Assert.Contains("year=2026", files[6]);
                Assert.Contains("year=2027", files[7]);
                Assert.Contains("year=2028", files[8]);
                Assert.Contains("year=2029", files[9]);

                var expectedFields = new[] { "company", "employee", documentIdColumn, ParquetTransformedItems.LastModifiedColumn };
                var newFile = files[5];
                using (var fs = File.OpenRead(newFile))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        Assert.True(field.Name.In(expectedFields));
                    }
                }
            }
        }

        [Fact]
        public async Task CanUpdateRunFrequency()
        {
            using (var store = GetDocumentStore())
            {
                var dt = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 5; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            Company = $"companies/{i}",
                            Employee = $"employees/{i}",
                            OrderedAt = dt,
                        };

                        await session.StoreAsync(o);

                        dt = dt.AddYears(1);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var orderDate = new Date(this.OrderedAt);

loadToOrders(partitionBy(['year', orderDate.getFullYear()]), 
{
    company: this.Company,
    employee: this.Employee
}
);
";

                var path = NewDataPath(forceCreateDir: true);

                var connectionStringName = $"{store.Database} to local";
                var configuration = new OlapEtlConfiguration
                {
                    Name = "olap-test",
                    ConnectionStringName = connectionStringName,
                    RunFrequency = "0 0 * * 0", // once a week
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

                var result = AddEtl(store, configuration, connectionString);
                var taskId = result.TaskId;

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories).OrderBy(x => x).ToList();

                Assert.Equal(5, files.Count);

                Assert.Contains("year=2020", files[0]);
                Assert.Contains("year=2021", files[1]);
                Assert.Contains("year=2022", files[2]);
                Assert.Contains("year=2023", files[3]);
                Assert.Contains("year=2024", files[4]);

                // update 

                configuration.RunFrequency = DefaultFrequency; // every minute
                store.Maintenance.Send(new UpdateEtlOperation<OlapConnectionString>(taskId, configuration));

                // add more data

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 6; i <= 10; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            Company = $"companies/{i}",
                            Employee = $"employees/{i}",
                            OrderedAt = dt,
                        };

                        await session.StoreAsync(o);

                        dt = dt.AddYears(1);
                    }

                    await session.SaveChangesAsync();
                }

                etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories).OrderBy(x => x).ToList();
                Assert.Equal(10, files.Count);

                Assert.Contains("year=2025", files[5]);
                Assert.Contains("year=2026", files[6]);
                Assert.Contains("year=2027", files[7]);
                Assert.Contains("year=2028", files[8]);
                Assert.Contains("year=2029", files[9]);
            }
        }

        [Fact]
        public async Task CanUpdateCustomPartitionValue()
        {
            using (var store = GetDocumentStore())
            {
                var dt = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 5; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            Company = $"companies/{i}",
                            Employee = $"employees/{i}",
                            OrderedAt = dt,
                        };

                        await session.StoreAsync(o);

                        dt = dt.AddYears(1);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var orderDate = new Date(this.OrderedAt);

loadToOrders(partitionBy(['year', orderDate.getFullYear()], ['location', $customPartitionValue]), 
{
    company: this.Company,
    employee: this.Employee
}
);
";

                var path = NewDataPath(forceCreateDir: true);

                var connectionStringName = $"{store.Database} to local";
                const string customPartition = "shop12";

                var configuration = new OlapEtlConfiguration
                {
                    Name = "olap-test",
                    ConnectionStringName = connectionStringName,
                    RunFrequency = DefaultFrequency,
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
                var connectionString = new OlapConnectionString
                {
                    Name = connectionStringName,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = path
                    }
                };

                var result = AddEtl(store, configuration, connectionString);
                var taskId = result.TaskId;

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories).ToList();

                Assert.Equal(5, files.Count);

                foreach (var file in files)
                {
                    Assert.Contains(customPartition, file);
                }

                // update 
                const string newCustomPartition = "shop35";
                configuration.CustomPartitionValue = newCustomPartition;
                store.Maintenance.Send(new UpdateEtlOperation<OlapConnectionString>(taskId, configuration));

                // add more data

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 6; i <= 10; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            Company = $"companies/{i}",
                            Employee = $"employees/{i}",
                            OrderedAt = dt,
                        };

                        await session.StoreAsync(o);

                        dt = dt.AddYears(1);
                    }

                    await session.SaveChangesAsync();
                }

                etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories).OrderBy(x => x).ToList();
                Assert.Equal(10, files.Count);

                foreach (var file in new[] { files[5], files[6], files[7], files[8], files[9] })
                {
                    Assert.Contains(newCustomPartition, file);
                }
            }
        }

        [Fact]
        public async Task CanUpdateLocalSettings()
        {
            using (var store = GetDocumentStore())
            {

                var dt = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 5; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            Company = $"companies/{i}",
                            Employee = $"employees/{i}",
                            OrderedAt = dt,
                        };

                        await session.StoreAsync(o);

                        dt = dt.AddYears(1);
                    }

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var orderDate = new Date(this.OrderedAt);

loadToOrders(partitionBy(['year', orderDate.getFullYear()]), 
{
    company: this.Company,
    employee: this.Employee
}
);
";

                var rootPath = NewDataPath();
                var path = Path.Combine(rootPath, "test_1");

                var connectionStringName = $"{store.Database} to local";

                var configuration = new OlapEtlConfiguration
                {
                    Name = "olap-test",
                    ConnectionStringName = connectionStringName,
                    RunFrequency = DefaultFrequency,
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
                var result = AddEtl(store, configuration, connectionString);
                var taskId = result.TaskId;

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories).OrderBy(x => x).ToList();

                Assert.Equal(5, files.Count);

                Assert.Contains("year=2020", files[0]);
                Assert.Contains("year=2021", files[1]);
                Assert.Contains("year=2022", files[2]);
                Assert.Contains("year=2023", files[3]);
                Assert.Contains("year=2024", files[4]);

                // disable task 

                configuration.Disabled = true;
                var update = store.Maintenance.Send(new UpdateEtlOperation<OlapConnectionString>(taskId, configuration));
                taskId = update.TaskId;
                Assert.NotNull(update.RaftCommandIndex);

                // update connection string
                var newPath = Path.Combine(rootPath, "test_2");
                connectionString.LocalSettings = new LocalSettings
                {
                    FolderPath = newPath
                };

                var putResult = store.Maintenance.Send(new PutConnectionStringOperation<OlapConnectionString>(connectionString));
                Assert.NotNull(putResult.RaftCommandIndex);

                // re enable task

                configuration.Disabled = false;
                update = store.Maintenance.Send(new UpdateEtlOperation<OlapConnectionString>(taskId, configuration));
                Assert.NotNull(update.RaftCommandIndex);

                // add more data

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 6; i <= 10; i++)
                    {
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            Company = $"companies/{i}",
                            Employee = $"employees/{i}",
                            OrderedAt = dt,
                        };

                        await session.StoreAsync(o);

                        dt = dt.AddYears(1);
                    }

                    await session.SaveChangesAsync();
                }

                etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                files = Directory.GetFiles(newPath, searchPattern: AllFilesPattern, SearchOption.AllDirectories).OrderBy(x => x).ToList();
                Assert.Equal(5, files.Count);

                Assert.Contains("year=2025", files[0]);
                Assert.Contains("year=2026", files[1]);
                Assert.Contains("year=2027", files[2]);
                Assert.Contains("year=2028", files[3]);
                Assert.Contains("year=2029", files[4]);
            }
        }

        [Fact]
        public async Task LastModifiedShouldBeMillisecondsSinceUnixEpoch()
        {
            using (var store = GetDocumentStore())
            {

                var dt = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Query.Order
                    {
                        Id = "orders/1",
                        Company = "companies/1",
                        OrderedAt = dt,
                    });

                    await session.SaveChangesAsync();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var orderDate = new Date(this.OrderedAt);

loadToOrders(partitionBy(['year', orderDate.getFullYear()]), 
{
    company: this.Company
}
);
";
                var path = NewDataPath();

                SetupLocalOlapEtl(store, script, path);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                string[] files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);

                Assert.Equal(1, files.Length);

                DateTime? lastModifiedDateTime;
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Order>("orders/1");
                    lastModifiedDateTime = session.Advanced.GetLastModifiedFor(doc);
                    Assert.True(lastModifiedDateTime.HasValue);
                }

                var expectedFields = new[] { "company", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = new ParquetReader(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        Assert.True(field.Name.In(expectedFields));
                        if (field.Name != ParquetTransformedItems.LastModifiedColumn)
                            continue;

                        var expected = ParquetTransformedItems.UnixTimestampFromDateTime(lastModifiedDateTime.Value);
                        var data = rowGroupReader.ReadColumn((DataField)field).Data;
                        Assert.True(data.Length == 1);

                        foreach (var val in data)
                        {
                            var l = (long)val;
                            Assert.Equal(expected, l);
                        }
                    }
                }
            }
        }

        private static string GenerateConfigurationScript(string path, out string command)
        {
            var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var localSetting = new LocalSettings
            {
                FolderPath = path
            };

            var localSettingsString = JsonConvert.SerializeObject(localSetting);

            string script;

            if (PlatformDetails.RunningOnPosix)
            {
                command = "bash";
                script = $"#!/bin/bash\r\necho '{localSettingsString}'";
                File.WriteAllText(scriptPath, script);
                Process.Start("chmod", $"700 {scriptPath}");
            }
            else
            {
                command = "powershell";
                script = $"echo '{localSettingsString}'";
                File.WriteAllText(scriptPath, script);
            }

            return scriptPath;
        }

        private void SetupLocalOlapEtl(DocumentStore store, string script, string path, string name = "olap-test", string frequency = null, string transformationName = null)
        {
            var connectionStringName = $"{store.Database} to local";
            var configuration = new OlapEtlConfiguration
            {
                Name = name,
                ConnectionStringName = connectionStringName,
                RunFrequency = frequency ?? DefaultFrequency,
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

            SetupLocalOlapEtl(store, configuration, path, connectionStringName);
        }

        private void SetupLocalOlapEtl(DocumentStore store, OlapEtlConfiguration configuration, string path, string connectionStringName)
        {
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

        private class User
        {
            public decimal Decimal { get; set; }

            public long Int64 { get; set; }

            public double Double { get; set; }

            public byte Byte { get; set; }

            public sbyte SByte { get; set; }

            public float Float { get; set; }

            public short Int16 { get; set; }

            public int Int32 { get; set; }

            public ushort UInt16 { get; set; }

            public uint UInt32 { get; set; }

            public ulong UInt64 { get; set; }
        }
    }
}
