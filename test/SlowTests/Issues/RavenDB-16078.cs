using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16078: RavenTestBase
    {
        public RavenDB_16078(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Id { get; set; }
            public int Order;
            public string Ref;
        }

        [Fact]
        public void CanStreamMultipleProjectionsOfSameValue()
        {
            using var store = GetDocumentStore();
            using (var s = store.OpenSession())
            {
                s.Store(new Item {Order = 1, Ref = "items/3"}, "items/1");
                s.Store(new Item {Order = 2, Ref = "items/3"}, "items/2");
                s.Store(new Item {Order = 3, Ref = "items/4"}, "items/3");
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                var q = s.Advanced.RawQuery<Item>("from Items as i load i.Ref as r select r");
                using var it = s.Advanced.Stream(q);
                var count = 0;
                while (it.MoveNext())
                {
                    count++;
                    if (count != 3)
                    {
                        Assert.Equal(3, it.Current.Document.Order);
                    }
                    else
                    {
                        // no match for "items/4", returns default value
                        Assert.Equal(0, it.Current.Document.Order);
                    }
                }
                Assert.Equal(3, count);
            }
        }

        [Fact]
        public void CanStreamMultipleProjectionsOfSameValue_AnotherNRE()
        {
            using var store = GetDocumentStore();
            using (var s = store.OpenSession())
            {
                s.Store(new Item { Id = "items/1",  Order = 1, Ref = "items/3" });
                s.Store(new Item { Id = "items/2", Order = 2, Ref = "items/1" });
                s.Store(new Item { Id = "items/3", Order = 3, Ref = "items/1" });
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                var q = s.Advanced.RawQuery<Item>("from Items as i load i.Ref as r select r");
                using var it = s.Advanced.Stream(q);
                var count = 0;
                while (it.MoveNext())
                {
                    count++;
                    if (it.Current.Document.Id == "items/1")
                    {
                        Assert.Equal(1, it.Current.Document.Order);
                    }
                    else if (it.Current.Document.Id == "items/2")
                    {
                        Assert.Equal(2, it.Current.Document.Order);
                    }
                    else
                    {
                        Assert.Equal(3, it.Current.Document.Order);
                    }
                }
                Assert.Equal(3, count);
            }
        }
    }
}
