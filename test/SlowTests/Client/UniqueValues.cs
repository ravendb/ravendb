using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
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

            await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
            await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);

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
    }
}
