using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Exceptions;
using SlowTests.Core.Utils.Entities.Faceted;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Faceted
{
    public class RavenDB_12748 : RavenTestBase
    {
        public RavenDB_12748(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDeserializeLegacyFacetSetup()
        {
            var facets = new DynamicJsonArray
            {
                new DynamicJsonValue
                {
                    [nameof(Facet.Aggregations)] = new DynamicJsonValue
                    {
                        [nameof(FacetAggregation.Min)] = "Test"
                    }
                }
            };
            var rangeFacets = new DynamicJsonArray
            {
                new DynamicJsonValue
                {
                    [nameof(Facet.Aggregations)] = new DynamicJsonValue
                    {
                        [nameof(FacetAggregation.Max)] = "Test2"
                    }
                }
            };

            var djv = new DynamicJsonValue
            {
                [nameof(FacetSetup.Facets)] = facets,
                [nameof(FacetSetup.RangeFacets)] = rangeFacets
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = context.ReadObject(djv, "facet/setup");
                var setup = FacetSetup.Create("myId", json);

                Assert.Equal("myId", setup.Id);

                Assert.Equal(1, setup.Facets.Count);
                Assert.Equal(1, setup.Facets[0].Aggregations.Count);
                Assert.Equal("Test", setup.Facets[0].Aggregations[FacetAggregation.Min].Single().Name);
                Assert.Null(setup.Facets[0].Aggregations[FacetAggregation.Min].Single().DisplayName);

                Assert.Equal(1, setup.RangeFacets.Count);
                Assert.Equal(1, setup.RangeFacets[0].Aggregations.Count);
                Assert.Equal("Test2", setup.RangeFacets[0].Aggregations[FacetAggregation.Max].Single().Name);
                Assert.Null(setup.RangeFacets[0].Aggregations[FacetAggregation.Max].Single().DisplayName);
            }
        }

        [Fact]
        public void CanCorrectlyAggregate()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3, Quantity = 3 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9, Quantity = 5 });
                    session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333, Quantity = 7777 });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order, Orders_All>()
                        .AggregateBy(f => f.ByField(x => x.Region))
                        .AndAggregateBy(f => f.ByField(x => x.Product).SumOn(x => x.Total).AverageOn(x => x.Total).SumOn(x => x.Quantity))
                        .Execute();

                    var facetResult = r["Region"];
                    Assert.Equal(1, facetResult.Values.Count);
                    Assert.Null(facetResult.Values.First().Name);
                    Assert.Equal(3, facetResult.Values.First().Count);

                    facetResult = r["Product"];
                    var totalValues = facetResult.Values.Where(x => x.Name == "Total").ToList();

                    Assert.Equal(2, totalValues.Count);

                    var milkValue = totalValues.First(x => x.Range == "milk");
                    var iphoneValue = totalValues.First(x => x.Range == "iphone");

                    Assert.Equal(2, milkValue.Count);
                    Assert.Equal(1, iphoneValue.Count);
                    Assert.Equal(12, milkValue.Sum);
                    Assert.Equal(3333, iphoneValue.Sum);
                    Assert.Equal(6, milkValue.Average);
                    Assert.Equal(3333, iphoneValue.Average);
                    Assert.Null(milkValue.Max);
                    Assert.Null(iphoneValue.Max);
                    Assert.Null(milkValue.Min);
                    Assert.Null(iphoneValue.Min);

                    var quantityValues = facetResult.Values.Where(x => x.Name == "Quantity").ToList();

                    milkValue = quantityValues.First(x => x.Range == "milk");
                    iphoneValue = quantityValues.First(x => x.Range == "iphone");

                    Assert.Equal(2, milkValue.Count);
                    Assert.Equal(1, iphoneValue.Count);
                    Assert.Equal(8, milkValue.Sum);
                    Assert.Equal(7777, iphoneValue.Sum);
                    Assert.Null(milkValue.Average);
                    Assert.Null(iphoneValue.Average);
                    Assert.Null(milkValue.Max);
                    Assert.Null(iphoneValue.Max);
                    Assert.Null(milkValue.Min);
                    Assert.Null(iphoneValue.Min);
                }

                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order, Orders_All>()
                        .AggregateBy(f => f.ByField(x => x.Region))
                        .AndAggregateBy(f => f.ByField(x => x.Product).SumOn(x => x.Total, "T1").AverageOn(x => x.Total, "T1").SumOn(x => x.Quantity, "Q1"))
                        .Execute();

                    var facetResult = r["Region"];
                    Assert.Equal(1, facetResult.Values.Count);
                    Assert.Null(facetResult.Values.First().Name);
                    Assert.Equal(3, facetResult.Values.First().Count);

                    facetResult = r["Product"];
                    var totalValues = facetResult.Values.Where(x => x.Name == "T1").ToList();

                    Assert.Equal(2, totalValues.Count);

                    var milkValue = totalValues.First(x => x.Range == "milk");
                    var iphoneValue = totalValues.First(x => x.Range == "iphone");

                    Assert.Equal(2, milkValue.Count);
                    Assert.Equal(1, iphoneValue.Count);
                    Assert.Equal(12, milkValue.Sum);
                    Assert.Equal(3333, iphoneValue.Sum);
                    Assert.Equal(6, milkValue.Average);
                    Assert.Equal(3333, iphoneValue.Average);
                    Assert.Null(milkValue.Max);
                    Assert.Null(iphoneValue.Max);
                    Assert.Null(milkValue.Min);
                    Assert.Null(iphoneValue.Min);

                    var quantityValues = facetResult.Values.Where(x => x.Name == "Q1").ToList();

                    milkValue = quantityValues.First(x => x.Range == "milk");
                    iphoneValue = quantityValues.First(x => x.Range == "iphone");

                    Assert.Equal(2, milkValue.Count);
                    Assert.Equal(1, iphoneValue.Count);
                    Assert.Equal(8, milkValue.Sum);
                    Assert.Equal(7777, iphoneValue.Sum);
                    Assert.Null(milkValue.Average);
                    Assert.Null(iphoneValue.Average);
                    Assert.Null(milkValue.Max);
                    Assert.Null(iphoneValue.Max);
                    Assert.Null(milkValue.Min);
                    Assert.Null(iphoneValue.Min);
                }

                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order, Orders_All>()
                        .AggregateBy(f => f.ByField(x => x.Region))
                        .AndAggregateBy(f => f.ByField(x => x.Product)
                            .SumOn(x => x.Total, "T1")
                            .SumOn(x => x.Total, "T2")
                            .AverageOn(x => x.Total, "T2")
                            .SumOn(x => x.Quantity, "Q1"))
                        .Execute();

                    var facetResult = r["Region"];
                    Assert.Equal(1, facetResult.Values.Count);
                    Assert.Null(facetResult.Values.First().Name);
                    Assert.Equal(3, facetResult.Values.First().Count);

                    facetResult = r["Product"];
                    var totalValues = facetResult.Values.Where(x => x.Name == "T1").ToList();

                    Assert.Equal(2, totalValues.Count);

                    var milkValue = totalValues.First(x => x.Range == "milk");
                    var iphoneValue = totalValues.First(x => x.Range == "iphone");

                    Assert.Equal(2, milkValue.Count);
                    Assert.Equal(1, iphoneValue.Count);
                    Assert.Equal(12, milkValue.Sum);
                    Assert.Equal(3333, iphoneValue.Sum);
                    Assert.Null(milkValue.Average);
                    Assert.Null(iphoneValue.Average);
                    Assert.Null(milkValue.Max);
                    Assert.Null(iphoneValue.Max);
                    Assert.Null(milkValue.Min);
                    Assert.Null(iphoneValue.Min);

                    totalValues = facetResult.Values.Where(x => x.Name == "T2").ToList();

                    Assert.Equal(2, totalValues.Count);

                    milkValue = totalValues.First(x => x.Range == "milk");
                    iphoneValue = totalValues.First(x => x.Range == "iphone");

                    Assert.Equal(2, milkValue.Count);
                    Assert.Equal(1, iphoneValue.Count);
                    Assert.Equal(12, milkValue.Sum);
                    Assert.Equal(3333, iphoneValue.Sum);
                    Assert.Equal(6, milkValue.Average);
                    Assert.Equal(3333, iphoneValue.Average);
                    Assert.Null(milkValue.Max);
                    Assert.Null(iphoneValue.Max);
                    Assert.Null(milkValue.Min);
                    Assert.Null(iphoneValue.Min);

                    var quantityValues = facetResult.Values.Where(x => x.Name == "Q1").ToList();

                    milkValue = quantityValues.First(x => x.Range == "milk");
                    iphoneValue = quantityValues.First(x => x.Range == "iphone");

                    Assert.Equal(2, milkValue.Count);
                    Assert.Equal(1, iphoneValue.Count);
                    Assert.Equal(8, milkValue.Sum);
                    Assert.Equal(7777, iphoneValue.Sum);
                    Assert.Null(milkValue.Average);
                    Assert.Null(iphoneValue.Average);
                    Assert.Null(milkValue.Max);
                    Assert.Null(iphoneValue.Max);
                    Assert.Null(milkValue.Min);
                    Assert.Null(iphoneValue.Min);
                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_Ranges()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3, Quantity = 3 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9, Quantity = 5 });
                    session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333, Quantity = 7777 });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                        .AggregateBy(f => f
                            .ByRanges(
                                x => x.Total < 100,
                                x => x.Total >= 100 && x.Total < 500,
                                x => x.Total >= 500 && x.Total < 1500,
                                x => x.Total >= 1500)
                            .SumOn(x => x.Total).AverageOn(x => x.Total).SumOn(x => x.Quantity))
                        .Execute();

                    var facetResult = r["Total"];
                    Assert.Equal(8, facetResult.Values.Count);

                    var range1 = facetResult.Values.Single(x => x.Range == "Total < 100.0" && x.Name == "Total");
                    var range2 = facetResult.Values.Single(x => x.Range == "Total >= 1500.0" && x.Name == "Total");

                    Assert.Equal(2, range1.Count);
                    Assert.Equal(1, range2.Count);
                    Assert.Equal(12, range1.Sum);
                    Assert.Equal(3333, range2.Sum);
                    Assert.Equal(6, range1.Average);
                    Assert.Equal(3333, range2.Average);
                    Assert.Null(range1.Max);
                    Assert.Null(range2.Max);
                    Assert.Null(range1.Min);
                    Assert.Null(range2.Min);

                    range1 = facetResult.Values.Single(x => x.Range == "Total < 100.0" && x.Name == "Quantity");
                    range2 = facetResult.Values.Single(x => x.Range == "Total >= 1500.0" && x.Name == "Quantity");

                    Assert.Equal(2, range1.Count);
                    Assert.Equal(1, range2.Count);
                    Assert.Equal(8, range1.Sum);
                    Assert.Equal(7777, range2.Sum);
                    Assert.Null(range1.Average);
                    Assert.Null(range2.Average);
                    Assert.Null(range1.Max);
                    Assert.Null(range2.Max);
                    Assert.Null(range1.Min);
                    Assert.Null(range2.Min);
                }

                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                        .AggregateBy(f => f
                            .ByRanges(
                                x => x.Total < 100,
                                x => x.Total >= 100 && x.Total < 500,
                                x => x.Total >= 500 && x.Total < 1500,
                                x => x.Total >= 1500)
                            .SumOn(x => x.Total, "T1").AverageOn(x => x.Total, "T1").SumOn(x => x.Quantity, "Q1"))
                        .Execute();

                    var facetResult = r["Total"];
                    Assert.Equal(8, facetResult.Values.Count);

                    var range1 = facetResult.Values.Single(x => x.Range == "Total < 100.0" && x.Name == "T1");
                    var range2 = facetResult.Values.Single(x => x.Range == "Total >= 1500.0" && x.Name == "T1");

                    Assert.Equal(2, range1.Count);
                    Assert.Equal(1, range2.Count);
                    Assert.Equal(12, range1.Sum);
                    Assert.Equal(3333, range2.Sum);
                    Assert.Equal(6, range1.Average);
                    Assert.Equal(3333, range2.Average);
                    Assert.Null(range1.Max);
                    Assert.Null(range2.Max);
                    Assert.Null(range1.Min);
                    Assert.Null(range2.Min);

                    range1 = facetResult.Values.Single(x => x.Range == "Total < 100.0" && x.Name == "Q1");
                    range2 = facetResult.Values.Single(x => x.Range == "Total >= 1500.0" && x.Name == "Q1");

                    Assert.Equal(2, range1.Count);
                    Assert.Equal(1, range2.Count);
                    Assert.Equal(8, range1.Sum);
                    Assert.Equal(7777, range2.Sum);
                    Assert.Null(range1.Average);
                    Assert.Null(range2.Average);
                    Assert.Null(range1.Max);
                    Assert.Null(range2.Max);
                    Assert.Null(range1.Min);
                    Assert.Null(range2.Min);
                }

                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                        .AggregateBy(f => f
                            .ByRanges(
                                x => x.Total < 100,
                                x => x.Total >= 100 && x.Total < 500,
                                x => x.Total >= 500 && x.Total < 1500,
                                x => x.Total >= 1500)
                            .SumOn(x => x.Total, "T1").SumOn(x => x.Total, "T2").AverageOn(x => x.Total, "T2").SumOn(x => x.Quantity, "Q1"))
                        .Execute();

                    var facetResult = r["Total"];
                    Assert.Equal(12, facetResult.Values.Count);

                    var range1 = facetResult.Values.Single(x => x.Range == "Total < 100.0" && x.Name == "T1");
                    var range2 = facetResult.Values.Single(x => x.Range == "Total >= 1500.0" && x.Name == "T1");

                    Assert.Equal(2, range1.Count);
                    Assert.Equal(1, range2.Count);
                    Assert.Equal(12, range1.Sum);
                    Assert.Equal(3333, range2.Sum);
                    Assert.Null(range1.Average);
                    Assert.Null(range2.Average);
                    Assert.Null(range1.Max);
                    Assert.Null(range2.Max);
                    Assert.Null(range1.Min);
                    Assert.Null(range2.Min);

                    range1 = facetResult.Values.Single(x => x.Range == "Total < 100.0" && x.Name == "T2");
                    range2 = facetResult.Values.Single(x => x.Range == "Total >= 1500.0" && x.Name == "T2");

                    Assert.Equal(2, range1.Count);
                    Assert.Equal(1, range2.Count);
                    Assert.Equal(12, range1.Sum);
                    Assert.Equal(3333, range2.Sum);
                    Assert.Equal(6, range1.Average);
                    Assert.Equal(3333, range2.Average);
                    Assert.Null(range1.Max);
                    Assert.Null(range2.Max);
                    Assert.Null(range1.Min);
                    Assert.Null(range2.Min);

                    range1 = facetResult.Values.Single(x => x.Range == "Total < 100.0" && x.Name == "Q1");
                    range2 = facetResult.Values.Single(x => x.Range == "Total >= 1500.0" && x.Name == "Q1");

                    Assert.Equal(2, range1.Count);
                    Assert.Equal(1, range2.Count);
                    Assert.Equal(8, range1.Sum);
                    Assert.Equal(7777, range2.Sum);
                    Assert.Null(range1.Average);
                    Assert.Null(range2.Average);
                    Assert.Null(range1.Max);
                    Assert.Null(range2.Max);
                    Assert.Null(range1.Min);
                    Assert.Null(range2.Min);
                }
            }
        }

        [Fact]
        public void ShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    using (store.GetRequestExecutor().UsingClientVersion("4.2.3.42"))
                    {
                        var e = Assert.Throws<InvalidQueryException>(() => session.Advanced
                            .RawQuery<dynamic>("from index 'Orders/All' select facet(playerId, sum(goals), sum(errors), sum(assists))")
                            .ToList());

                        Assert.Contains("Detected duplicate facet aggregation", e.Message);
                    }
                }
            }
        }

        private class Orders_All : AbstractIndexCreationTask<Order>
        {
            public Orders_All()
            {
                Map = orders =>
                    from order in orders
                    select new
                    {
                        order.Currency,
                        order.Product,
                        order.Total,
                        order.Quantity,
                        order.Region,
                        order.At,
                        order.Tax
                    };
            }
        }
    }
}
