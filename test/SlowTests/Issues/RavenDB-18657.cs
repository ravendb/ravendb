using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Google.Apis.Json;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18657 : RavenTestBase
    {
        public RavenDB_18657(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Get_Fields_With_Different_Casing()
        {
            using var store = GetDocumentStore();

            var user = new User
            {
                Name = "Grisha",
                name = "Shahar"
            };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenSession())
            {
                var query = session.Query<User>().ProjectInto<User>();
                Assert.Equal("from 'Users' select Name, name", query.ToString());
                var results = query.ToList();
                Assert.Equal(user.Name, results.First().Name);
                Assert.Equal(user.name, results.First().name);
            }
        }

        [Fact]
        public async Task DocumentQuery_RQL_Expression_Should_Not_Include_Private_Members()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Item(11));
                session.SaveChanges();
            }

            var index = new Item_Content();
            await index.ExecuteAsync(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var query = session.Query<Item, Item_Content>().ProjectInto<Item>();
                Assert.Equal("from index 'Item/Content' select content, Content", query.ToString());

                var l = query.ToList();

                Assert.Equal(12, l.First().Content);
            }
        }

        public class Item
        {

            public Item()
            {
            }

            public Item(int a)
            {
                content = a;
            }


            private int? content
            {
                get;
                set;
            }

            public int? Content
            {
                get => content;
                set => content = value == int.MinValue ? null : value;
            }
        }


        private class Item_Content : AbstractIndexCreationTask<Item>
        {
            public Item_Content()
            {
                Map = (items) =>
                    from item in items
                    select new
                    {
                        Content = item.Content + 1
                    };


                Store(x => x.Content, FieldStorage.Yes);
            }
        }

        private class User
        {
            public string Name { get; set; }

            public string name { get; set; }
        }
    }
}
