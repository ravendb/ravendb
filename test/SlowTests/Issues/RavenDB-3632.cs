using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3632 : RavenTestBase
    {
        public RavenDB_3632(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task IncludeWithLoadAsync()
        {
            using (var store = GetDocumentStore())
            {
                string listId;
                using (var session = store.OpenAsyncSession())
                {
                    var item1 = new ListItem<string> { };
                    var item2 = new ListItem<string> { };
                    await session.StoreAsync(item1);
                    await session.StoreAsync(item2);
                    var list = new List<string, string> { Items = new[] { item1.Id, item2.Id } };
                    await session.StoreAsync(list);
                    listId = list.Id;
                    await session.SaveChangesAsync();

                }

                using (var session = store.OpenAsyncSession())
                {
                    var list = await session.Include<List<string, string>, ListItem<string>>(l => l.Items).LoadAsync<List<string, string>>(listId);
                    var enumer = list.Items.Select(it => it.ToString());
                    var list2 = await session.LoadAsync<ListItem<string>>(enumer);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task IncludeWithIdSetAndLoadAsync()
        {
            using (var store = GetDocumentStore())
            {
                var listId = Guid.NewGuid().ToString();
                using (var session = store.OpenAsyncSession())
                {
                    var item1 = new ListItem<string> { Id = Guid.NewGuid().ToString() };
                    var item2 = new ListItem<string> { Id = Guid.NewGuid().ToString() };
                    var list = new List<string, string> { Id = listId, Items = new[] { item1.Id, item2.Id } };
                    await session.StoreAsync(item1);
                    await session.StoreAsync(item2);
                    await session.StoreAsync(list);
                    await session.SaveChangesAsync();

                }

                using (var session = store.OpenAsyncSession())
                {
                    var list = await session.Include<List<string, string>, ListItem<string>>(l => l.Items).LoadAsync<List<string, string>>(listId);
                    var enumer = list.Items.Select(it => it.ToString());
                    var l2 = await session.LoadAsync<ListItem<string>>(enumer);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        private class List<T, T1>
        {
            public T Id { get; set; }
            public IEnumerable<T1> Items { get; set; }
        }

        private class ListItem<T>
        {
            public T Id { get; set; }
        }
    }
}

