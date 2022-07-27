using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using EmbeddedTests;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class UniqueValues : RavenTestBase
    {
        public UniqueValues(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanPutUniqueString(Options options)
        {
            DoNotReuseServer();
            using var store = GetDocumentStore(options);
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("test", "Karmel", 0));
            var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("test"));
            Assert.Equal("Karmel", res.Value);
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanPutUniqueObject(Options options)
        {
            DoNotReuseServer();
            using var store = GetDocumentStore(options);
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0));
            Assert.True(res.Successful);
            Assert.Equal("Karmel", res.Value.Name);
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanPutMultiDifferentValues(Options options)
        {
            DoNotReuseServer();
            using var store = GetDocumentStore(options);
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0));
            var res2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test2", new User
            {
                Name = "Karmel"
            }, 0));

            Assert.Equal("Karmel", res.Value.Name);
            Assert.True(res.Successful);
            Assert.Equal("Karmel", res2.Value.Name);
            Assert.True(res2.Successful);
        }

        [Fact]
        public async Task CanExportAndImportCmpXchg()
        {
            var file = GetTempFileName();
            DoNotReuseServer();
            var store = GetDocumentStore();
            var store2 = GetDocumentStore();
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0));
            var res2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test2", new User
            {
                Name = "Karmel"
            }, 0));

            Assert.Equal("Karmel", res.Value.Name);
            Assert.True(res.Successful);
            Assert.Equal("Karmel", res2.Value.Name);
            Assert.True(res2.Successful);

            var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var result = await store2.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("test"));
            Assert.Equal("Karmel", result.Value.Name);
            Assert.True(res.Successful);

            result = await store2.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("test2"));
            Assert.Equal("Karmel", result.Value.Name);
            Assert.True(res.Successful);
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanListCompareExchange(Options options)
        {
            DoNotReuseServer();
            using var store = GetDocumentStore(options);
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0));
            var res2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test2", new User
            {
                Name = "Karmel"
            }, 0));

            Assert.Equal("Karmel", res.Value.Name);
            Assert.True(res.Successful);
            Assert.Equal("Karmel", res2.Value.Name);
            Assert.True(res2.Successful);

            var values = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<User>("test"));
            Assert.Equal(2, values.Count);
            Assert.Equal("Karmel", values["test"].Value.Name);
            Assert.Equal("Karmel", values["test2"].Value.Name);
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanRemoveUnique(Options options)
        {
            DoNotReuseServer();
            using var store = GetDocumentStore(options);

            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("test", "Karmel", 0));
            Assert.Equal("Karmel", res.Value);
            Assert.True(res.Successful);

            res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<string>("test", res.Index));
            Assert.True(res.Successful);
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RemoveUniqueFailed(Options options)
        {
            DoNotReuseServer();
            using var store = GetDocumentStore(options);

            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("test", "Karmel", 0));
            Assert.Equal("Karmel", res.Value);
            Assert.True(res.Successful);

            res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<string>("test", 0));
            Assert.False(res.Successful);

            var result = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("test"));
            Assert.Equal("Karmel", result.Value);
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReturnCurrentValueWhenPuttingConcurrently(Options options)
        {
            DoNotReuseServer();
            using var store = GetDocumentStore(options);
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0));
            var res2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Name = "Karmel2"
            }, 0));
            Assert.True(res.Successful);
            Assert.False(res2.Successful);
            Assert.Equal("Karmel", res.Value.Name);
            Assert.Equal("Karmel", res2.Value.Name);

            res2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Name = "Karmel2"
            }, res2.Index));
            ;
            Assert.True(res2.Successful);
            Assert.Equal("Karmel2", res2.Value.Name);
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanGetIndexValue(Options options)
        {
            DoNotReuseServer();
            using var store = GetDocumentStore(options);
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0));
            var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("test"));
            Assert.Equal("Karmel", res.Value.Name);

            var res2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Name = "Karmel2"
            }, res.Index));
            Assert.True(res2.Successful);
            Assert.Equal("Karmel2", res2.Value.Name);
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanListValues(Options options)
        {
            DoNotReuseServer();
            using var store = GetDocumentStore(options);
            for (var i = 0; i < 10; i++)
            {
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test" + i, new User
                {
                    Name = "value" + i
                }, 0));
            }

            var result = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<User>("", 0, 3));
            Assert.Equal(new HashSet<string> { "test0", "test1", "test2" }, result.Keys.ToHashSet());

            result = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<User>("", 1, 3));
            Assert.Equal(new HashSet<string> { "test1", "test2", "test3" }, result.Keys.ToHashSet());

            result = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<User>("", 8, 5));
            Assert.Equal(new HashSet<string> { "test8", "test9" }, result.Keys.ToHashSet());

            // add some values at the beginning and at the end
            for (var i = 0; i < 2; i++)
            {
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("a" + i, new User
                {
                    Name = "value" + i
                }, 0));
            }

            for (var i = 0; i < 2; i++)
            {
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("z" + i, new User
                {
                    Name = "value" + i
                }, 0));
            }

            // now query with prefix

            result = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<User>("test", 0, 3));
            Assert.Equal(new HashSet<string> { "test0", "test1", "test2" }, result.Keys.ToHashSet());

            result = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<User>("test", 1, 3));
            Assert.Equal(new HashSet<string> { "test1", "test2", "test3" }, result.Keys.ToHashSet());

            result = await store.Operations.SendAsync(new GetCompareExchangeValuesOperation<User>("test", 8, 5));
            Assert.Equal(new HashSet<string> { "test8", "test9" }, result.Keys.ToHashSet());
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task SaveSameValuesToDifferentDatabases(Options options)
        {
            DoNotReuseServer();
            var store = GetDocumentStore(caller: $"CmpExchangeTest1-{new Guid()}");
            var store2 = GetDocumentStore(caller: $"CmpExchangeTest2-{new Guid()}");
            var user = new User { Name = "Karmel" };
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", user, 0));
            var res2 = await store2.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", user, 0));
            Assert.Equal("Karmel", res.Value.Name);
            Assert.Equal("Karmel", res2.Value.Name);
            Assert.True(res.Successful);
            Assert.True(res2.Successful);
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CompareExchangeShouldBeRemovedFromStorageWhenDbGetsDeleted(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var user = new User
                {
                    Name = "💩"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/poo", user, 0));

                var dbName = store.Database;
                var stats = store.Maintenance.ForDatabase(dbName).Send(new GetDetailedStatisticsOperation());

                Assert.Equal(1, stats.CountOfCompareExchange);
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, hardDelete: false));

                int resultItems = 0;
                using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var result = Server.ServerStore.Cluster.GetCompareExchangeFromPrefix(ctx, dbName, 0, int.MaxValue);
                    foreach (var item in result)
                        resultItems++;
                }

                Assert.Equal(0, resultItems);
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CompareExchangeTombstoneShouldBeRemovedFromStorageWhenDbGetsDeleted(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var user = new User
                {
                    Name = "🤡"
                };
                var cxRes = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/clown", user, 0));

                var dbName = store.Database;
                var stats = store.Maintenance.ForDatabase(dbName).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(1, stats.CountOfCompareExchange);
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("emojis/clown", cxRes.Index));
                stats = store.Maintenance.ForDatabase(dbName).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(0, stats.CountOfCompareExchange);

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, hardDelete: false));
                int resultItems = 0;
                using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var result = Server.ServerStore.Cluster.GetCompareExchangeTombstonesByKey(ctx, dbName);
                    foreach (var item in result)
                        resultItems++;
                }

                Assert.Equal(0, resultItems);
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task EnsureCanCompareExchangeWithCorrectCharsEscapeInKey(Options options)
        {
            DoNotReuseServer();
            using var store = GetDocumentStore(options);
            var strList = new List<string>()
            {
                "emails/foo+test@email.com", "fĀthēr&s0n", @"Aa0 !""#$%'()*+,-./:;<=>?@[\]^_`{|}~", "ĀĒĪŌŪAa0 322", "āēīōūBb1 123"
            };

            foreach (var str in strList)
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>(str, "Karmel", 0));

            var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
            var numOfCmpXchg = stats.CountOfCompareExchange;
            Assert.Equal(5, numOfCmpXchg);

            foreach (var str in strList)
            {
                var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>(str));
                Assert.Equal(str, res.Key);
                Assert.Equal("Karmel", res.Value);

                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<string>(str, res.Index));
            }

            var finalStats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
            var realNumOfCmpXchg = finalStats.CountOfCompareExchange;
            Assert.Equal(0, realNumOfCmpXchg);
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanAddMetadataToSimpleCompareExchange(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var str = "Test";
                var num = 123.456;
                var key = "egr/test/cmp/x/change/simple";
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var result = session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, 322);
                    result.Metadata["TestString"] = str;
                    result.Metadata["TestNumber"] = num;
                    await session.SaveChangesAsync();
                }

                var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<int>(key));
                Assert.NotNull(res.Metadata);
                Assert.Equal(322, res.Value);
                Assert.Equal(str, res.Metadata["TestString"]);
                Assert.Equal(num, res.Metadata["TestNumber"]);

                var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                Assert.Equal(1, stats.CountOfCompareExchange);
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanAddMetadataToCompareExchangeAndWaitForExpiration(Options options)
        {
            using var server = GetNewServer();

            options.Server = server;

            using (var store = GetDocumentStore(options))
            {
                var user = new User
                {
                    Name = "EGOR"
                };

                var dateTime = DateTime.Now.AddMinutes(2);
                var str = "Test";
                var num = 123.456;
                var key = "egr/test/cmp/x/change";
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var result = session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, user);
                    result.Metadata[Constants.Documents.Metadata.Expires] = dateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff");
                    result.Metadata["TestString"] = str;
                    result.Metadata["TestNumber"] = num;
                    await session.SaveChangesAsync();
                }

                var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>(key));
                Assert.Equal(user.Name, res.Value.Name);
                Assert.NotNull(res.Metadata);
                Assert.Equal(dateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff"), res.Metadata[Constants.Documents.Metadata.Expires]);
                Assert.Equal(str, res.Metadata["TestString"]);
                Assert.Equal(num, res.Metadata["TestNumber"]);

                server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);

                var val = await WaitForValueAsync(async () =>
                {
                    var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchange;
                }, 0);

                Assert.Equal(0, val);
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanAddMetadataToSimpleCompareExchangeAndWaitForExpiration(Options options)
        {
            using var server = GetNewServer();

            options.Server = server;

            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                var dateTime = DateTime.Now.AddMinutes(2);
                var str = "Test";
                var num = 123.456;
                var key = "egr/test/cmp/x/change";
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var result = session.Advanced.ClusterTransaction.CreateCompareExchangeValue<string>(key, "EGR");
                    result.Metadata[Constants.Documents.Metadata.Expires] = dateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff");
                    result.Metadata["TestString"] = str;
                    result.Metadata["TestNumber"] = num;
                    await session.SaveChangesAsync();
                }

                var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>(key));
                Assert.Equal("EGR", res.Value);
                Assert.NotNull(res.Metadata);
                Assert.Equal(dateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff"), res.Metadata[Constants.Documents.Metadata.Expires]);
                Assert.Equal(str, res.Metadata["TestString"]);
                Assert.Equal(num, res.Metadata["TestNumber"]);

                server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);

                var val = await WaitForValueAsync(async () =>
                {
                    var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchange;
                }, 0);

                Assert.Equal(0, val);
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanAddMetadataToIntCompareExchangeAndWaitForExpiration(Options options)
        {
            using var server = GetNewServer();

            options.Server = server;

            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                var dateTime = DateTime.Now.AddMinutes(2);
                var str = "Test";
                var num = 123.456;
                var key = "egr/test/cmp/x/change";
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var result = session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, 322);
                    result.Metadata[Constants.Documents.Metadata.Expires] = dateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff");
                    result.Metadata["TestString"] = str;
                    result.Metadata["TestNumber"] = num;
                    await session.SaveChangesAsync();
                }

                var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<int>(key));
                Assert.Equal(322, res.Value);
                Assert.NotNull(res.Metadata);
                Assert.Equal(dateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff"), res.Metadata[Constants.Documents.Metadata.Expires]);
                Assert.Equal(str, res.Metadata["TestString"]);
                Assert.Equal(num, res.Metadata["TestNumber"]);

                server.ServerStore.Observer.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);

                var val = await WaitForValueAsync(async () =>
                {
                    var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchange;
                }, 0);

                Assert.Equal(0, val);
            }
        }

        [Fact]
        public async Task CanImportCompareExchangeWithoutMetadata()
        {
            var dummyDump = SmugglerTests.CreateDummyDump(1);
            var key = "EGR";
            var value = 322;
            var compareExchangeList = new List<DynamicJsonValue>();
            compareExchangeList.Add(new DynamicJsonValue()
            {
                ["Key"] = key,
                ["Value"] = "{\"Object\":" + value + "}"
            });
            dummyDump["CompareExchange"] = new DynamicJsonArray(compareExchangeList);
            using (var store = GetDocumentStore())
            {
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                using (var bjro = ctx.ReadObject(dummyDump, "dump"))
                await using (var ms = new MemoryStream())
                await using (var zipStream = new GZipStream(ms, CompressionMode.Compress))
                {
                    await bjro.WriteJsonToAsync(zipStream);
                    zipStream.Flush();
                    ms.Position = 0;

                    var operation = await store.Smuggler.ForDatabase(store.Database).ImportAsync(new DatabaseSmugglerImportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Identities | DatabaseItemType.CompareExchange
                    }, ms);

                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));
                }

                var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                Assert.Equal(1, stats.CountOfCompareExchange);

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var res = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<int>(key);
                    Assert.NotNull(res.Metadata);
                    Assert.Empty(res.Metadata);
                    Assert.Equal(value, res.Value);
                }
            }
        }
    }
}
