using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8741 : RavenTestBase
    {
        [Fact]
        public void Can_group_by_array()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Tags = new []{ "a", "c"}
                    });

                    session.Store(new Item
                    {
                        Tags = new[] { "a", "b" }
                    });

                    session.Store(new Item
                    {
                        Tags = new[] { "a", "b" }
                    });

                    session.SaveChanges();

                    var results = session.Query<Item>().Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => x.Tags).Select(x =>
                        new
                        {
                            Count = x.Count(),
                            Tags = x.Key
                        }).
                        OrderBy(x => x.Count)
                        .ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal(new[] { "a", "c" }, results[0].Tags);
                    Assert.Equal(1, results[0].Count);

                    Assert.Equal(new[] { "a", "b" }, results[1].Tags);
                    Assert.Equal(2, results[1].Count);
                }
            }
        }

        [Fact]
        public void Can_group_by_lazy_double_value()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        DoubleVal = 1.1
                    });

                    session.Store(new Item
                    {
                        DoubleVal = 1.2
                    });

                    session.Store(new Item
                    {
                        DoubleVal = 1.2
                    });

                    session.SaveChanges();

                    var results = session.Query<Item>().Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => x.DoubleVal).Select(x =>
                            new
                            {
                                Count = x.Count(),
                                Value = x.Key
                            }).
                        OrderBy(x => x.Count)
                        .ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal(1.1, results[0].Value);
                    Assert.Equal(1, results[0].Count);

                    Assert.Equal(1.2, results[1].Value);
                    Assert.Equal(2, results[1].Count);
                }
            }
        }

        private class Item
        {
            public string[] Tags { get; set; }

            public double DoubleVal { get; set; }
        }
    }
}
