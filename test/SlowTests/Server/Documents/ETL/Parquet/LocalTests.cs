using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide.Operations;
using Sparrow.Extensions;
using Sparrow.Platform;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Parquet
{
    public class LocalTests : EtlTestBase
    {
        public LocalTests(ITestOutputHelper output) : base(output)
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
                    SetupLocalParquetEtl(store, script, path, TimeSpan.FromMinutes(10));

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    Thread.Sleep(20000);

                    var files = Directory.GetFiles(path);
                    Assert.Equal(2, files.Length);

                    var expectedFields = new[] { "OrderId", "ShipVia", "Company" };

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

        [Fact]
        public async Task SimpleTransformation2()
        {
            var path = GetTempPath("Orders");
            try
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


                    var script = @"
var o = {
    OrderId: id(this),
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

loadToOrders(key, o);
";

                    SetupLocalParquetEtl(store, script, path, TimeSpan.FromMinutes(10));

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] { "OrderId", "RequireAt", "Total" };

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

                            long count = 1;
                            foreach (var val in data)
                            {
                                switch (field.Name)
                                {
                                    case "OrderId":
                                        Assert.Equal($"orders/{count}", val);
                                        break;
                                    case "RequireAt":
                                        var expected = baseline.AddDays(count).AddDays(7).GetDefaultRavenFormat(isUtc: true);
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

        [Fact]
        public async Task CanHandleMissingFieldsOnSomeDocuments()
        {
            var path = GetTempPath("Orders");
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

                            if (i % 2 == 0)
                                o.Freight = i;

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);


                    var script = @"
var o = {
    OrderId: id(this),
    Company : this.Company
};

if (this.Freight > 0)
    o.Freight = this.Freight

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(key, o);
";

                    SetupLocalParquetEtl(store, script, path, TimeSpan.FromMinutes(10));

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] {"OrderId", "Company", "Freight" };

                    using (var fs = File.OpenRead(files[0]))
                    using (var parquetReader = new ParquetReader(fs))
                    {
                        Assert.Equal(1, parquetReader.RowGroupCount);
                        Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                        using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                        foreach (var field in  parquetReader.Schema.Fields)
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

        [Fact]
        public async Task CanHandleNullFieldValuesOnSomeDocument()
        {
            var path = GetTempPath("Orders");
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
    OrderId: id(this),
    Company : this.Company
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(key, o);
";

                    SetupLocalParquetEtl(store, script, path, TimeSpan.FromMinutes(10));

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] { "OrderId", "Company" };

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

                            var count = 1;
                            foreach (var val in data)
                            {
                                switch (field.Name)
                                {
                                    case "OrderId":
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

        [Fact]
        public async Task ShouldRespectEtlBatchFrequency()
        {
            var path = GetTempPath("Orders");
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
var o = {
    OrderId: id(this),
    Company : this.Company
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(key, o);
";

                    SetupLocalParquetEtl(store, script, path, TimeSpan.FromMinutes(1));

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    var expectedFields = new[] { "OrderId", "Company" };

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

                    await Task.Delay(1000);

                    baseline = new DateTime(2021, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 20; i <= 30; i++)
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

                    var sw = new Stopwatch();
                    sw.Start();
                    etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);
                    etlDone.Wait(TimeSpan.FromSeconds(60));
                    var timeWaited = sw.Elapsed.TotalMilliseconds;
                    sw.Stop();

                    var tolerance = TimeSpan.FromSeconds(58).TotalMilliseconds;
                    Assert.True(timeWaited >= tolerance);

                    files = Directory.GetFiles(path);
                    Assert.Equal(2, files.Length);
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

        [Fact]
        public async Task CanUseSettingFromScript()
        {
            var path = GetTempPath("Orders");
            try
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
    OrderId: id(this),
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

loadToOrders(key, o);
";

                    var connectionStringName = $"{store.Database} to S3";

                    var scriptPath = GenerateConfigurationScript(path, out string command);
                    var connectionString = new ParquetEtlConnectionString
                    {
                        Name = connectionStringName,
                        LocalSettings = new ParquetEtlLocalSettings
                        {
                            GetBackupConfigurationScript = new GetBackupConfigurationScript
                            {
                                Exec = command, 
                                Arguments = scriptPath
                            }
                        }
                    };

                    var configuration = new ParquetEtlConfiguration
                    {
                        Name = connectionStringName,
                        ConnectionStringName = connectionStringName,
                        ETLFrequency = TimeSpan.FromMinutes(5),
                        Transforms =
                        {
                            new Transformation
                            {
                                Name = "MonthlyOrders",
                                Collections = new List<string> {"Orders"},
                                Script = @"
var o = {
    OrderId: id(this),
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

loadToOrders(key, o);
"
                            }
                        }
                    };

                    AddEtl(store, configuration, connectionString);

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);
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

        [Fact]
        public async Task AfterDatabaseRestartEtlShouldRespectFrequency()
        {
            var path = GetTempPath("Orders");
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
var o = {
    OrderId: id(this),
    Company : this.Company
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(key, o);
";

                    SetupLocalParquetEtl(store, script, path, TimeSpan.FromMinutes(1));

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    var files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    // disable an re enable the database

                    var result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
                    Assert.True(result.Success);
                    Assert.True(result.Disabled);

                    result = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));
                    Assert.True(result.Success);
                    Assert.False(result.Disabled);

                    baseline = new DateTime(2021, 1, 1);

                    // add more data
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 20; i <= 30; i++)
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

                    // assert

                    var sw = new Stopwatch();
                    sw.Start();
                    etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    Assert.False(etlDone.Wait(TimeSpan.FromSeconds(10)));
                    files = Directory.GetFiles(path);
                    Assert.Equal(1, files.Length);

                    Assert.True(etlDone.Wait(TimeSpan.FromSeconds(60)));

                    var timeWaited = sw.Elapsed.TotalMilliseconds;
                    sw.Stop();

                    var tolerance = TimeSpan.FromSeconds(2).TotalMilliseconds;
                    var expected = TimeSpan.FromSeconds(60).TotalMilliseconds;

                    Assert.True(timeWaited >= expected - tolerance);

                    files = Directory.GetFiles(path);
                    Assert.Equal(2, files.Length);
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

        private static string GenerateConfigurationScript(string path, out string command)
        {
            var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var localSetting = new ParquetEtlLocalSettings
            {
                FolderPath = path, 
                KeepFilesOnDisc = true
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

        private static string GetTempPath(string collection, [CallerMemberName] string caller = null)
        {
            var tmpPath = Path.GetTempPath();
            return Directory.CreateDirectory(Path.Combine(tmpPath, caller, collection)).FullName;
        }

        protected void SetupLocalParquetEtl(DocumentStore store, string script, string path, TimeSpan frequency)
        {
            var connectionStringName = $"{store.Database} to S3";

            AddEtl(store, new ParquetEtlConfiguration
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
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
            }, new ParquetEtlConnectionString
            {
                Name = connectionStringName,
                LocalSettings = new ParquetEtlLocalSettings
                {
                    FolderPath = path,
                    KeepFilesOnDisc = true
                }
            });
        }
    }
}
