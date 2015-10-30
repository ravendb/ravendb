using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests
{
    public class RavenDB_3632 : RavenTestBase
    {
        [Fact]
        public async Task includeWithLoadAsync()
        {
            using (var server = GetNewServer())
            {
                using (var store = new DocumentStore { Url = server.SystemDatabase.Configuration.ServerUrl }.Initialize())
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
        }

        [Fact]
        public async Task includeWithIdSetAndLoadAsync()
        {
            using (var server = GetNewServer())
            {
                using (var store = new DocumentStore { Url = server.SystemDatabase.Configuration.ServerUrl }.Initialize())
                {

                    var listId = Guid.NewGuid();
                    using (var session = store.OpenAsyncSession())
                    {
                        var item1 = new ListItem<Guid> { Id = Guid.NewGuid() };
                        var item2 = new ListItem<Guid> { Id = Guid.NewGuid() };
                        var list = new List<Guid, Guid> { Id = listId, Items = new[] { item1.Id, item2.Id } };
                        await session.StoreAsync(item1);
                        await session.StoreAsync(item2);
                        await session.StoreAsync(list);
                        await session.SaveChangesAsync();

                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        var list = await session.Include<List<Guid, Guid>, ListItem<Guid>>(l => l.Items).LoadAsync<List<Guid, Guid>>(listId);
                        var enumer = list.Items.Select(it => it.ToString());
                        var l2 = await session.LoadAsync<ListItem<Guid>>(enumer);
                        Assert.Equal(1, session.Advanced.NumberOfRequests);
                    }
                }
            }
        }

        public class List<T, T1>
        {
            public T Id { get; set; }
            public IEnumerable<T1> Items { get; set; }
        }

        public class ListItem<T>
        {
            public T Id { get; set; }
        }
    }
}

