using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    public class ShardedEtlTests : ShardedTestBase
    {
        public ShardedEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        internal const string DefaultFrequency = "* * * * *"; // every minute
        private const string AllFilesPattern = "*.*";


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
/*

        [Fact]
        public async Task SqlEtl_SimpleTransformation()
        {
            using (var store = GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    CreateRdbmsSchema(connectionString);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order
                        {
                            OrderLines = new List<OrderLine>
                            {
                                new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                            }
                        });
                        await session.SaveChangesAsync();
                    }

                    var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, defaultScript);

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
        }*/

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
    }
}
