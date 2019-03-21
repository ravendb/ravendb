using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Client
{
    public class UniqueValues : RavenTestBase
    {
        [Fact]
        public async Task CanPutUniqueString()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("test", "Karmel", 0));
            var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("test"));
            Assert.Equal("Karmel", res.Value);
        }

        [Fact]
        public async Task CanPutUniqueObject()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", new User
            {
                Name = "Karmel"
            }, 0));
            Assert.True(res.Successful);
            Assert.Equal("Karmel", res.Value.Name);
        }

        [Fact]
        public async Task CanPutMultiDifferentValues()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
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

        [Fact]
        public async Task CanListCompareExchange()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
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
        
        [Fact]
        public async Task CanRemoveUnique()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("test", "Karmel", 0));
            Assert.Equal("Karmel", res.Value);
            Assert.True(res.Successful);
            
            res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<string>("test", res.Index));
            Assert.True(res.Successful);
        }
        
        [Fact]
        public async Task RemoveUniqueFailed()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("test", "Karmel", 0));
            Assert.Equal("Karmel", res.Value);
            Assert.True(res.Successful);
            
            res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<string>("test", 0));
            Assert.False(res.Successful);
            
            var result = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("test"));
            Assert.Equal("Karmel", result.Value);
        }
        
        [Fact]
        public async Task ReturnCurrentValueWhenPuttingConcurrently()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
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

        [Fact]
        public async Task CanGetIndexValue()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
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
        
        [Fact]
        public async Task CanListValues()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            for (var  i = 0; i < 10; i++)
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
            for (var  i = 0; i < 2; i++)
            {
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("a" + i, new User
                {
                    Name = "value" + i
                }, 0));
            }
            
            for (var  i = 0; i < 2; i++)
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

        [Fact]
        public async Task SaveSameValuesToDifferentDatabases()
        {
            DoNotReuseServer();
            var store = GetDocumentStore(caller: $"CmpExchangeTest1-{new Guid()}");
            var store2 = GetDocumentStore(caller: $"CmpExchangeTest2-{new Guid()}");
            var user  = new User{Name = "Karmel"};
            var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test",user,0));
            var res2 = await store2.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("test", user, 0));
            Assert.Equal("Karmel", res.Value.Name);
            Assert.Equal("Karmel", res2.Value.Name);
            Assert.True(res.Successful);
            Assert.True(res2.Successful);
        }

        [Fact]
        public async Task CompareExchangeShouldBeRemovedFromStorageWhenDbGetsDeleted()
        {
            using (var store = GetDocumentStore())
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
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var result = Server.ServerStore.Cluster.GetCompareExchangeValuesStartsWith(ctx, dbName, dbName, 0, int.MaxValue);
                    foreach (var item in result)
                        resultItems++;
                }

                Assert.Equal(0, resultItems);
            }
        }

        [Fact]
        public async Task EnsureCanCompareExchangeWithCorrectCharsEscapeInKey()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();

            const string cmpXchgKey1 = "emails/foo+test@email.com";
            const string cmpXchgKey2 = "fĀthēr&s0n";
            const string cmpXchgKey3 = @"Aa0 !""#$%'()*+,-./:;<=>?@[\]^_`{|}~";
            const string cmpXchgKey4 = "ĀĒĪŌŪAa0 322";
            const string cmpXchgKey5 = "āēīōūBb1 123";

            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>(cmpXchgKey1, "Karmel", 0));
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>(cmpXchgKey2, "Karmel", 0));
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>(cmpXchgKey3, "Karmel", 0));
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>(cmpXchgKey4, "Karmel", 0));
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>(cmpXchgKey5, "Karmel", 0));

            var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
            var numOfCmpXchg = stats.CountOfCompareExchange;
            Assert.Equal(5, numOfCmpXchg);

            var res1 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>(cmpXchgKey1));
            var res2 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>(cmpXchgKey2));
            var res3 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>(cmpXchgKey3));
            var res4 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>(cmpXchgKey4));
            var res5 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>(cmpXchgKey5));

            Assert.Equal(cmpXchgKey1, res1.Key);
            Assert.Equal(cmpXchgKey2, res2.Key);
            Assert.Equal(cmpXchgKey3, res3.Key);
            Assert.Equal(cmpXchgKey4, res4.Key);
            Assert.Equal(cmpXchgKey5, res5.Key);
            Assert.Equal("Karmel", res1.Value);
            Assert.Equal("Karmel", res2.Value);
            Assert.Equal("Karmel", res3.Value);
            Assert.Equal("Karmel", res4.Value);
            Assert.Equal("Karmel", res5.Value);

            await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<string>(cmpXchgKey1, res1.Index));
            await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<string>(cmpXchgKey2, res2.Index));
            await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<string>(cmpXchgKey3, res3.Index));
            await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<string>(cmpXchgKey4, res4.Index));
            await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<string>(cmpXchgKey5, res5.Index));

            var finalStats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
            var realNumOfCmpXchg = finalStats.CountOfCompareExchange;
            Assert.Equal(0, realNumOfCmpXchg);
        }
    }
}
