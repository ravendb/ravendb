using System;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client
{
    public class UniqueValues : RavenTestBase
    {
        [Fact]
        public async Task CanPutUniqueString()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            await store.Operations.CompareExchangeAsync("test", "Karmel", 0);
            var res = await store.Operations.GetCompareExchangeValueAsync<string>("test");
            Assert.Equal("Karmel", res.Value);
        }

        [Fact]
        public async Task CanPutUniqueObject()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            var res = await store.Operations.CompareExchangeAsync("test", new User
            {
                Name = "Karmel"
            }, 0);
            Assert.Equal("Karmel", res.Value.Name);
        }

        [Fact]
        public async Task CanPutMultiDifferentValues()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            var res = await store.Operations.CompareExchangeAsync("test", new User
            {
                Name = "Karmel"
            }, 0);
            var res2 = await store.Operations.CompareExchangeAsync("test2", new User
            {
                Name = "Karmel"
            }, 0);

            Assert.Equal("Karmel", res.Value.Name);
            Assert.Equal("Karmel", res2.Value.Name);
        }

        [Fact]
        public async Task ReturnCurrentValueWhenPuttingConcurrently()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            var res = await store.Operations.CompareExchangeAsync("test", new User
            {
                Name = "Karmel"
            }, 0);
            var res2 = await store.Operations.CompareExchangeAsync("test", new User
            {
                Name = "Karmel2"
            }, 0);
            Assert.Equal("Karmel", res.Value.Name);
            Assert.Equal("Karmel", res2.Value.Name);

            res2 = await store.Operations.CompareExchangeAsync("test", new User
            {
                Name = "Karmel2"
            }, res2.Index);
            Assert.Equal("Karmel2", res2.Value.Name);
        }

        [Fact]
        public async Task CanGetIndexValue()
        {
            DoNotReuseServer();
            var store = GetDocumentStore();
            await store.Operations.CompareExchangeAsync("test", new User
            {
                Name = "Karmel"
            }, 0);
            var res = await store.Operations.GetCompareExchangeValueAsync<User>("test");
            Assert.Equal("Karmel", res.Value.Name);

            var res2 = await store.Operations.CompareExchangeAsync("test", new User
            {
                Name = "Karmel2"
            }, res.Index);
            Assert.Equal("Karmel2", res2.Value.Name);
        }

        [Fact]
        public async Task SaveSameValuesToDifferentDatabases()
        {
            DoNotReuseServer();
            var store = GetDocumentStore(caller: $"CmpExchangeTest1-{new Guid()}");
            var store2 = GetDocumentStore(caller: $"CmpExchangeTest2-{new Guid()}");
            var user  = new User{Name = "Karmel"};
            var res = await store.Operations.CompareExchangeAsync("test", user, 0);
            var res2 = await store2.Operations.CompareExchangeAsync("test", user, 0);
            Assert.Equal("Karmel", res.Value.Name);
            Assert.Equal("Karmel", res2.Value.Name);
        }
    }
}
