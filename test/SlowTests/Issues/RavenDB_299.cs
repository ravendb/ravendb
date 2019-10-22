// -----------------------------------------------------------------------
//  <copyright file="RavenDB_299.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_299 : RavenTestBase
    {
        public RavenDB_299(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public Tag[] Tags { get; set; }
        }

        private class Tag
        {
            public string Name;
        }

        private class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items =>
                      items.SelectMany(x => x.Tags, (item, tag) => new { tag.Name });
            }
        }

        [Fact]
        public void CanWorkWithSelectManyOverload()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Tags = new[] { new Tag { Name = "test" }, }
                    });
                    session.SaveChanges();
                }
                new Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Item, Index>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .ToList());
                }
            }
        }
    }
}
