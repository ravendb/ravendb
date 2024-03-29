﻿using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18657 : RavenTestBase
    {
        public RavenDB_18657(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task Can_Get_Fields_With_Different_Casing(Options options)
        {
            using var store = GetDocumentStore(options);

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
                var documentQuery = session.Advanced.DocumentQuery<User>().SelectFields<User>();
                Assert.Equal("from 'Users' select Name, name", query.ToString()); // Name and name are public properties so it should contain them.
                Assert.Equal("from 'Users' select Name, name", documentQuery.ToString());
                var results = query.ToList();
                Assert.Equal(user.Name, results.First().Name);
                Assert.Equal(user.name, results.First().name);
                results = documentQuery.ToList();
                Assert.Equal(user.Name, results.First().Name);
                Assert.Equal(user.name, results.First().name);
            }
        }

        private class User
        {
            public string Name { get; set; }

            public string name { get; set; }
        }


        [Fact]
        public async Task DocumentQuery_RQL_Expression_Should_Not_Include_Private_Members()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Item(11, 22));
                session.SaveChanges();
            }

            var index = new Item_Content();
            await index.ExecuteAsync(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var query = session.Query<Item, Item_Content>().ProjectInto<Item>();
                var documentQuery = session.Advanced.DocumentQuery<Item, Item_Content>().SelectFields<Item>();
                Assert.Equal("from index 'Item/Content' select Content", query.ToString()); // 'content', 'content2' are private so it should contain only 'Content'
                Assert.Equal("from index 'Item/Content' select Content", documentQuery.ToString());
                var l = query.ToList();
                Assert.Equal(12, l.First().Content);
                l = documentQuery.ToList();
                Assert.Equal(12, l.First().Content);
            }
        }

        public class Item
        {

            public Item()
            {
            }

            public Item(int c, int c2)
            {
                content = c;
                content2 = c2;
            }

            private int? content2
            {
                get;
                set;
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
        
    }
}
