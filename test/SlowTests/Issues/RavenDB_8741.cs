using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;
using Raven.Client.Documents;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8741 : RavenTestBase
    {
        public RavenDB_8741(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_group_by_array_and_collection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Tags = new []{ "a", "c"},
                        Tags2 = new List<string>{ "a", "c"}
                    });

                    session.Store(new Item
                    {
                        Tags = new[] { "a", "b" },
                        Tags2 = new List<string>{ "a", "b"}
                    });

                    session.Store(new Item
                    {
                        Tags = new[] { "a", "b" },
                        Tags2 = new List<string>{ "a", "b"}
                    });

                    session.SaveChanges();

                    // by entire array content
                    var results = session.Query<Item>().Customize(x => x.WaitForNonStaleResults())
                        .GroupByArrayContent(x => x.Tags).Select(x =>
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

                    // by individual array values
                    var results2 = session.Query<Item>().Customize(x => x.WaitForNonStaleResults())
                        .GroupByArrayValues(x => x.Tags).Select(x =>
                        new
                        {
                            Count = x.Count(),
                            Tag = x.Key
                        }).
                        OrderBy(x => x.Count)
                        .ToList();

                    Assert.Equal(3, results2.Count);

                    Assert.Equal("c", results2[0].Tag);
                    Assert.Equal(1, results2[0].Count);

                    Assert.Equal("b", results2[1].Tag);
                    Assert.Equal(2, results2[1].Count);

                    Assert.Equal("a", results2[2].Tag);
                    Assert.Equal(3, results2[2].Count);

                    var results3 = session.Query<Item>().Customize(x => x.WaitForNonStaleResults())
                        .GroupByArrayValues(x => x.Tags2).Select(x =>
                            new
                            {
                                Count = x.Count(),
                                Tag = x.Key
                            }).
                        OrderBy(x => x.Count)
                        .ToList();

                    Assert.Equal(3, results3.Count);

                    Assert.Equal("c", results3[0].Tag);
                    Assert.Equal(1, results3[0].Count);

                    Assert.Equal("b", results3[1].Tag);
                    Assert.Equal(2, results3[1].Count);

                    Assert.Equal("a", results3[2].Tag);
                    Assert.Equal(3, results3[2].Count);
                }
            }
        }

        [Fact]
        public void Should_throw_on_group_by_collection_use_dedicated_methods_instead()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ex = Assert.Throws<InvalidOperationException>(() => session.Query<Item>().Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => x.Tags).Select(x =>
                            new
                            {
                                Count = x.Count(),
                                Tags = x.Key
                            })
                        .ToList());

                    Assert.Equal("Please use one of dedicated methods to group by collection: GroupByArrayValues, GroupByArrayContent. Field name: Tags", ex.Message);
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

            public List<string> Tags2 { get; set; }

            public double DoubleVal { get; set; }
        }
    }
}
