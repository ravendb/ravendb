// -----------------------------------------------------------------------
//  <copyright file="RavenDB_301.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_301 : RavenNewTestBase
    {
        [Fact]
        public void CanUseTertiaryIncludes()
        {
            using (var store = GetDocumentStore())
            {
                new Index().Execute(store);
                new IndexTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Name = "Oren",
                        Parent = null
                    });
                    session.Store(new Item
                    {
                        Name = "Ayende",
                        Parent = "items/1"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var a = session.Query<Item, Index>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .TransformWith<IndexTransformer, Item>()
                        .Single(x => x.Name == "Ayende");

                    session.Load<Item>(a.Parent);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        private class Item
        {
            public string Name { get; set; }
            public string Id { get; set; }
            public string Parent { get; set; }
        }

        private class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items => from item in items
                               select new { item.Name };
            }
        }

        private class IndexTransformer : AbstractTransformerCreationTask<Item>
        {
            public IndexTransformer()
            {
                TransformResults = items =>
                                   from item in items
                                   let _ = Include(item.Parent)
                                   select new
                                   {
                                       Name = item.Name,
                                       Parent = item.Parent
                                   };
            }
        }
    }
}
