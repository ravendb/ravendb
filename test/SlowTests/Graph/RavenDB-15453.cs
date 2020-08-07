using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Graph
{
    public class RavenDB_15453 : RavenTestBase
    {
        public RavenDB_15453(ITestOutputHelper output) : base(output)
        {
        }

        public class Item
        {
            public string[] Links;
        }

        [Fact]
        public void CanGetSingleShortestPath()
        {
            using var store = GetDocumentStore();
            using (var s = store.OpenSession())
            {
                s.Store(new Item { Links = new[] { "items/2", "items/3" } }, "items/1");
                s.Store(new Item { Links = new[] { "items/1", "items/2" } }, "items/2");
                s.Store(new Item { Links = new[] { "items/2", "items/4" } }, "items/3");
                s.Store(new Item { Links = new[] { "items/1", "items/2" } }, "items/4");
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                var r = s.Advanced.RawQuery<dynamic>(@"
match (Items as Src where id() == 'items/1')
 -recursive as Path (shortest){
        [Links]->(Items as d)
 }-[Links]->(Items as Dst where id() == 'items/2')")
                    .ToList();
                Assert.Equal(1, r.Count);
                Assert.Equal(1, r[0].Path.Count);
            }
        }

        [Fact]
        public void CanGetSingleLongestPath()
        {
            using var store = GetDocumentStore();
            using (var s = store.OpenSession())
            {
                s.Store(new Item { Links = new[] { "items/2", "items/3" } }, "items/1");
                s.Store(new Item { Links = new[] { "items/1", "items/2" } }, "items/2");
                s.Store(new Item { Links = new[] { "items/2", "items/4" } }, "items/3");
                s.Store(new Item { Links = new[] { "items/1", "items/2" } }, "items/4");
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                var r = s.Advanced.RawQuery<dynamic>(@"
match (Items as Src where id() == 'items/1')
 -recursive as Path (longest){
        [Links]->(Items as d)
 }-[Links]->(Items as Dst where id() == 'items/2')")
                    .ToList();
                Assert.Equal(1, r.Count);
                Assert.Equal(3, r[0].Path.Count);
            }
        }
    }
}
