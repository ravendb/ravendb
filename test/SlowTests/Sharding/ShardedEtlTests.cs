using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using FastTests.Sharding;
using Parquet;
using Parquet.Data;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.ETL.Olap;
using SlowTests.Server.Documents.ETL.SQL;
using SlowTests.Server.Documents.Migration;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding
{
    public class ShardedEtlTests : ShardedTestBase
    {
        public ShardedEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string DefaultFrequency = "* * * * *"; // every minute
        private const string AllFilesPattern = "*.*";

        private const string DefaultSqlScript = @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    orderData.TotalCost += line.PricePerUnit;
    loadToOrderLines({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}

loadToOrders(orderData);
";

        [Fact]
        public void RavenEtl_Unsharded_Destination()
        {
            using (var src = GetShardedDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Server.ServerStore.Engine.Timeout.Disable = true;

                SetupRavenEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                const string id = "users/1";

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Joe Doe"
                    }, id);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(id);

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete(id);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(id);

                    Assert.Null(user);
                }
            }
        }

        [Fact]
        public void RavenEtl_Unsharded_Destination2()
        {
            using (var src = GetShardedDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Server.ServerStore.Engine.Timeout.Disable = true;

                SetupRavenEtl(src, dest, "Users", script: null);

                var etlsDone = WaitForEtlOnAllShards(src, (n, s) => s.LoadSuccesses > 0);
                var dbRecord = src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database)).Result;
                var shardedCtx = new ShardedContext(Server.ServerStore, dbRecord);
                var ids = new[] {"users/0", "users/4", "users/1"};

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    for (int i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var shardIndex = shardedCtx.GetShardIndex(context, id);
                        Assert.Equal(i, shardIndex);
                    }
                }

                using (var session = src.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        session.Store(new User
                        {
                            Name = "User" + i
                        }, id);
                    }

                    session.SaveChanges();
                }

                var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
                WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var user = session.Load<User>(id);

                        Assert.NotNull(user);
                        Assert.Equal("User" + i, user.Name);
                    }
                }

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Delete(ids[1]);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(ids[1]);

                    Assert.Null(user);
                }
            }
        }

        [Fact]
        public void RavenEtl_Sharded_Destination()
        {
            using (var src = GetShardedDocumentStore())
            using (var dest = GetShardedDocumentStore())
            {
                Server.ServerStore.Engine.Timeout.Disable = true;

                SetupRavenEtl(src, dest, "Users", script: null);

                var etlsDone = WaitForEtlOnAllShards(src, (n, s) => s.LoadSuccesses > 0);
                var dbRecord = src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database)).Result;
                var shardedCtx = new ShardedContext(Server.ServerStore, dbRecord);
                var ids = new[] { "users/0", "users/4", "users/1" };

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    for (int i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var shardIndex = shardedCtx.GetShardIndex(context, id);
                        Assert.Equal(i, shardIndex);
                    }
                }

                using (var session = src.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        session.Store(new User
                        {
                            Name = "User" + i
                        }, id);
                    }

                    session.SaveChanges();
                }

                var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
                WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var user = session.Load<User>(id);

                        Assert.NotNull(user);
                        Assert.Equal("User" + i, user.Name);
                    }
                }

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Delete(ids[1]);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(ids[1]);

                    Assert.Null(user);
                }
            }
        }

        [Fact]
        public void RavenEtl_Loading_to_different_collections()
        {
            using (var src = GetShardedDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlsDone = WaitForEtlOnAllShards(src, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupRavenEtl(src, dest, "users", @"
loadToUsers(this);
loadToPeople({Name: this.Name + ' ' + this.LastName });
loadToAddresses(load(this.AddressId));
");
                const int count = 5;

                using (var session = src.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        session.Store(new User
                        {
                            Age = i,
                            Name = "James",
                            LastName = "Smith",
                            AddressId = $"addresses/{i}$users{i}"
                        }, $"users/{i}");

                        session.Store(new Address
                        {
                            City = "New York"
                        }, $"addresses/{i}$users{i}"); // ensure that address doc is on the same shard as user
                    }

                    session.SaveChanges();
                }

                var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
                WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));
                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < count; i++)
                    {
                        var user = session.Load<User>($"users/{i}");
                        Assert.NotNull(user);
                        Assert.Equal("James", user.Name);
                        Assert.Equal("Smith", user.LastName);

                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.Equal("Users", metadata[Constants.Documents.Metadata.Collection]);

                        var person = session.Advanced.LoadStartingWith<Person>($"users/{i}/people/")[0];
                        Assert.NotNull(person);
                        Assert.Equal("James Smith", person.Name);

                        metadata = session.Advanced.GetMetadataFor(person);
                        Assert.Equal("People", metadata[Constants.Documents.Metadata.Collection]);

/*                        var address = session.Advanced.LoadStartingWith<Address>($"users/{i}/addresses/")[0];
                        Assert.NotNull(address);
                        Assert.Equal("New York", address.City);

                        metadata = session.Advanced.GetMetadataFor(address);
                        Assert.Equal("Addresses", metadata[Constants.Documents.Metadata.Collection]);*/
                    }
                }

                var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(10, stats.CountOfDocuments);

                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                using (var session = src.OpenSession())
                {
                    session.Delete("users/3");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/3");
                    Assert.Null(user);

                    var persons = session.Advanced.LoadStartingWith<Person>("users/3/people/");
                    Assert.Equal(0, persons.Length);

/*                    var addresses = session.Advanced.LoadStartingWith<Address>("users/3/addresses/");
                    Assert.Equal(0, addresses.Length);*/
                }

                stats = dest.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(7, stats.CountOfDocuments);
            }
        }

        [Fact]
        public async Task SqlEtl_SimpleTransformation()
        {
            using (var store = GetShardedDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    SqlEtlTests.CreateRdbmsSchema(connectionString);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order
                        {
                            Lines = new List<OrderLine>
                            {
                                new OrderLine
                                {
                                    PricePerUnit = 3, Product = "Milk", Quantity = 3
                                }, 
                                new OrderLine
                                {
                                    PricePerUnit = 4, Product = "Beer", Quantity = 2
                                },
                            }
                        });

                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, DefaultSqlScript);

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                            Assert.Equal(1, dbCommand.ExecuteScalar());
                            dbCommand.CommandText = " SELECT COUNT(*) FROM OrderLines";
                            Assert.Equal(2, dbCommand.ExecuteScalar());
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task SqlEtl_ReplicateMultipleBatches()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    SqlEtlTests.CreateRdbmsSchema(connectionString);
                    int testCount = 5000;

                    using (var bulkInsert = store.BulkInsert())
                    {
                        for (int i = 0; i < testCount; i++)
                        {
                            await bulkInsert.StoreAsync(new Order
                            {
                                Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        PricePerUnit = 3, Product = "Milk", Quantity = 3
                                    },
                                    new OrderLine
                                    {
                                        PricePerUnit = 4, Product = "Beer", Quantity = 2
                                    },
                                }
                            });
                        }
                    }

                    var etlDone = WaitForEtl(store, (n, s) => SqlEtlTests.GetOrdersCount(connectionString) == testCount);

                    SetupSqlEtl(store, connectionString, DefaultSqlScript);

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    Assert.Equal(testCount, SqlEtlTests.GetOrdersCount(connectionString));
                }
            }
        }

        [Fact]
        public async Task OlapEtl_Local_Destination()
        {
            using (var store = GetShardedDocumentStore())
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

                var etlsDone = WaitForEtlOnAllShards(store, (n, statistics) => statistics.LoadSuccesses != 0);

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

                var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
                WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(3, files.Length);

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

/*                        var data = rowGroupReader.ReadColumn((DataField)field).Data;
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

                        }*/
                    }
                }
            }
        }

        [AmazonS3Fact]
        public async Task OlapEtl_S3_Destination()
        {
            const string salesTableName = "Sales";
            var settings = GetS3Settings();

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


                    SetupS3OlapEtl(store, script, settings);
                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var s3Client = new RavenAwsS3Client(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/Orders";
                        var cloudObjects = await s3Client.ListObjectsAsync(prefix, string.Empty, false);

                        Assert.Equal(2, cloudObjects.FileInfoDetails.Count);
                        Assert.Contains("2020-01-01", cloudObjects.FileInfoDetails[0].FullPath);
                        Assert.Contains("2020-02-01", cloudObjects.FileInfoDetails[1].FullPath);

                        var fullPath = cloudObjects.FileInfoDetails[0].FullPath.Replace("=", "%3D");
                        var blob = await s3Client.GetObjectAsync(fullPath);

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

                    using (var s3Client = new RavenAwsS3Client(settings))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{salesTableName}";
                        var cloudObjects = await s3Client.ListObjectsAsync(prefix, string.Empty, false);

                        Assert.Equal(2, cloudObjects.FileInfoDetails.Count);
                        Assert.Contains("2020-01-01", cloudObjects.FileInfoDetails[0].FullPath);
                        Assert.Contains("2020-02-01", cloudObjects.FileInfoDetails[1].FullPath);

                        var fullPath = cloudObjects.FileInfoDetails[1].FullPath.Replace("=", "%3D");
                        var blob = await s3Client.GetObjectAsync(fullPath);

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
                await S3Tests.DeleteObjects(settings, salesTableName);
            }
        }

        private static AddEtlOperationResult SetupRavenEtl(IDocumentStore src, IDocumentStore dst, string collection, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null)
        {
            var connectionStringName = $"{src.Database}@{src.Urls.First()} to {dst.Database}@{dst.Urls.First()}";

            return AddEtl(src, new RavenEtlConfiguration
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>{ collection },
                            Script = script,
                            ApplyToAllDocuments = applyToAllDocuments,
                            Disabled = disabled
                        }
                    },
                    MentorNode = mentor,
                },
                new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dst.Database,
                    TopologyDiscoveryUrls = dst.Urls,
                }
            );
        }

        private AddEtlOperationResult SetupLocalOlapEtl(IDocumentStore store, string script, string path, string name = "olap-test", string frequency = null, string transformationName = null)
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

            var connectionString = new OlapConnectionString
            {
                Name = connectionStringName,
                LocalSettings = new LocalSettings
                {
                    FolderPath = path
                }
            };

            return AddEtl(store, configuration, connectionString);
        }

        private AddEtlOperationResult SetupS3OlapEtl(IDocumentStore store, string script, S3Settings settings, string customPartitionValue = null, string transformationName = null)
        {
            var connectionStringName = $"{store.Database} to S3";

            var configuration = new OlapEtlConfiguration
            {
                Name = "olap-s3-test",
                ConnectionStringName = connectionStringName,
                RunFrequency = DefaultFrequency,
                CustomPartitionValue = customPartitionValue,
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
            return AddEtl(store, configuration, new OlapConnectionString
            {
                Name = connectionStringName,
                S3Settings = settings
            });
        }

        private AddEtlOperationResult SetupSqlEtl(IDocumentStore store, string connectionString, string script, bool insertOnly = false, List<string> collections = null)
        {
            var connectionStringName = $"{store.Database}@{store.Urls.First()} to SQL DB";

            var configuration = new SqlEtlConfiguration
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                SqlTables =
                {
                    new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id", InsertOnlyMode = insertOnly},
                    new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId", InsertOnlyMode = insertOnly},
                },
                Transforms =
                {
                    new Transformation()
                    {
                        Name = "OrdersAndLines",
                        Collections = collections ?? new List<string> {"Orders"},
                        Script = script
                    }
                }
            };

            return AddEtl(store, configuration, new SqlConnectionString
            {
                Name = connectionStringName,
                ConnectionString = connectionString,
                FactoryName = "System.Data.SqlClient"
            });
        }

        private static AddEtlOperationResult AddEtl<T>(IDocumentStore src, EtlConfiguration<T> configuration, T connectionString) where T : ConnectionString
        {
            var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);

            var addResult = src.Maintenance.Send(new AddEtlOperation<T>(configuration));
            return addResult;
        }

        private ManualResetEventSlim WaitForEtl(IDocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var dbs = Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database).ToList();

            var mre = new ManualResetEventSlim();
            foreach (var task in dbs)
            {
                var db = task.Result;
                db.EtlLoader.BatchCompleted += x =>
                {
                    if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                        mre.Set();
                };
            }

            return mre;
        }

        private IEnumerable<ManualResetEventSlim> WaitForEtlOnAllShards(IDocumentStore store, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var dbs = Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database).ToList();
            var list = new List<ManualResetEventSlim>(dbs.Count);
            foreach (var task in dbs)
            {
                var mre = new ManualResetEventSlim();
                list.Add(mre);

                var db = task.Result;
                db.EtlLoader.BatchCompleted += x =>
                {
                    if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                        mre.Set();
                };
            }

            return list;
        }

        private S3Settings GetS3Settings([CallerMemberName] string caller = null)
        {
            var s3Settings = AmazonS3FactAttribute.S3Settings;
            if (s3Settings == null)
                return null;

            var prefix = $"olap/tests/{nameof(ShardedEtlTests)}-{Guid.NewGuid()}";
            var remoteFolderName = prefix;
            if (string.IsNullOrEmpty(caller) == false)
                remoteFolderName = $"{remoteFolderName}/{caller}";

            if (string.IsNullOrEmpty(s3Settings.RemoteFolderName) == false)
                remoteFolderName = $"{s3Settings.RemoteFolderName}/{remoteFolderName}";

            return new S3Settings
            {
                BucketName = s3Settings.BucketName,
                RemoteFolderName = remoteFolderName,
                AwsAccessKey = s3Settings.AwsAccessKey,
                AwsSecretKey = s3Settings.AwsSecretKey,
                AwsRegionName = s3Settings.AwsRegionName
            };
        }
    }
}
