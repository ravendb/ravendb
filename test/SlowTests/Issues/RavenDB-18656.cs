using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18656 : RavenTestBase
    {
        public RavenDB_18656(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DocumentQueryRQLExpressionShouldNotIncludePrivateMembers()
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
                var q = session.Query<Item, Item_Content>()
                    .ProjectInto<Item>();
                var expr = q.Expression.ToString();

                Assert.DoesNotContain("privateContent", expr); // expression shouldn't contain privateContent & privateContent2
            }
        }



        public class Item
        {

            public Item()
            {
            }

            public Item(int a, int b)
            {
                privateContent = a;
                privateContent2 = b;
            }

            private int? privateContent;
            public int? Content
            {
                get => privateContent;
                set => privateContent = value == int.MinValue ? null : value;
            }

            private int? privateContent2;
        }


        class Item_Content : AbstractIndexCreationTask<Item>
        {
            public Item_Content()
            {
                Map = (items) =>
                    from item in items
                    select new
                    {
                        item.Content
                    };


                Store("Content", FieldStorage.Yes);
            }

        }
    }
}
