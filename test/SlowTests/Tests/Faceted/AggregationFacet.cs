// -----------------------------------------------------------------------
//  <copyright file="AggregationFacet.cs" company="Hibernating Rhinos LTD"> 
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Faceted
{
    public class AggregationFacet : RavenTestBase
    {
        public AggregationFacet(ITestOutputHelper output) : base(output)
        {
        }

        private class Car
        {
            public string Make { get; set; }
            public int Year { get; set; }
            public decimal Price { get; set; }
        }

        [Fact]
        public void CanHandleMaxFacet_LowLevel()
        {
            using (var store = GetDocumentStore())
            {
                CreateAggregationSampleData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Car>("Cars")
                        .Where(x => x.Year == 2011)
                        .AggregateBy(f => f.ByField(x => x.Make).MaxOn(x => x.Price))
                        .Execute();

                    Assert.Equal(3, results["Make"].Values.Count);
                    Assert.Equal(1400.3, results["Make"].Values.First(x => x.Range == "toyota").Max.Value);
                    Assert.Equal(4900.3, results["Make"].Values.First(x => x.Range == "ford").Max.Value);
                    Assert.Equal(1000.3, results["Make"].Values.First(x => x.Range == "hyundai").Max.Value);
                }

            }
        }

        [Fact]
        public void CanHandleCountFacet_HighLevel()
        {
            using (var store = GetDocumentStore())
            {
                CreateAggregationSampleData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Car>("Cars")
                                         .Where(car => car.Year == 2011)
                                         .AggregateBy(x => x.ByField(y => y.Make))
                                         .Execute();


                    Assert.Equal(3, results["Make"].Values.Count);
                    Assert.Equal(2, results["Make"].Values.First(x => x.Range == "toyota").Count);
                    Assert.Equal(2, results["Make"].Values.First(x => x.Range == "ford").Count);
                    Assert.Equal(1, results["Make"].Values.First(x => x.Range == "hyundai").Count);
                }
            }
        }

        [Fact]
        public void CanHandleMaxFacet_HighLevel()
        {
            using (var store = GetDocumentStore())
            {
                CreateAggregationSampleData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Car>("Cars")
                                         .Where(car => car.Year == 2011)
                                         .AggregateBy(f => f.ByField(x => x.Make).MaxOn(x => x.Price))
                                         .Execute();


                    Assert.Equal(3, results["Make"].Values.Count);
                    Assert.Equal(1400.3, results["Make"].Values.First(x => x.Range == "toyota").Max.Value);
                    Assert.Equal(4900.3, results["Make"].Values.First(x => x.Range == "ford").Max.Value);
                    Assert.Equal(1000.3, results["Make"].Values.First(x => x.Range == "hyundai").Max.Value);
                }
            }
        }

        [Fact]
        public void CanHandleMinFacet_HighLevel()
        {
            using (var store = GetDocumentStore())
            {
                CreateAggregationSampleData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Car>("Cars")
                                         .Where(car => car.Year == 2011)
                                         .AggregateBy(f => f.ByField(x => x.Make).MinOn(x => x.Price))
                                         .Execute();

                    Assert.Equal(3, results["Make"].Values.Count);
                    Assert.Equal(1000.3, results["Make"].Values.First(x => x.Range == "toyota").Min.Value);
                    Assert.Equal(900.3, results["Make"].Values.First(x => x.Range == "ford").Min.Value);
                    Assert.Equal(1000.3, results["Make"].Values.First(x => x.Range == "hyundai").Min.Value);
                }
            }
        }

        [Fact]
        public void CanHandleSumFacet_HighLevel()
        {
            using (var store = GetDocumentStore())
            {
                CreateAggregationSampleData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Car>("Cars")
                                         .Where(car => car.Year == 2011)
                                         .AggregateBy(f => f.ByField(x => x.Make).SumOn(x => x.Price))
                                         .Execute();


                    Assert.Equal(3, results["Make"].Values.Count);
                    Assert.Equal(2400.6, results["Make"].Values.First(x => x.Range == "toyota").Sum.Value);
                    Assert.Equal(5800.6, results["Make"].Values.First(x => x.Range == "ford").Sum.Value);
                    Assert.Equal(1000.3, results["Make"].Values.First(x => x.Range == "hyundai").Sum.Value);
                }
            }
        }

        [Fact]
        public void CanHandleAverageFacet_HighLevel()
        {
            using (var store = GetDocumentStore())
            {
                CreateAggregationSampleData(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Car>("Cars")
                                         .Where(car => car.Year == 2011)
                                         .AggregateBy(f => f.ByField(x => x.Make).AverageOn(x => x.Price))
                                         .Execute();

                    Assert.Equal(3, results["Make"].Values.Count);
                    Assert.Equal(2400.6 / 2, results["Make"].Values.First(x => x.Range == "toyota").Average.Value);
                    Assert.Equal(5800.6 / 2, results["Make"].Values.First(x => x.Range == "ford").Average.Value);
                    Assert.Equal(1000.3, results["Make"].Values.First(x => x.Range == "hyundai").Average.Value);
                }
            }
        }

        [Fact]
        public void CanHandleAverageFacetAsync_HighLevel()
        {
            using (var store = GetDocumentStore())
            {
                CreateAggregationSampleData(store);

                using (var session = store.OpenAsyncSession())
                {
                    var results = session.Query<Car>("Cars")
                                         .Where(car => car.Year == 2011)
                                         .AggregateBy(f => f.ByField(x => x.Make).AverageOn(x => x.Price))
                                         .ExecuteAsync();

                    Assert.Equal(3, results.Result["Make"].Values.Count);
                    Assert.Equal(2400.6 / 2, results.Result["Make"].Values.First(x => x.Range == "toyota").Average.Value);
                    Assert.Equal(5800.6 / 2, results.Result["Make"].Values.First(x => x.Range == "ford").Average.Value);
                    Assert.Equal(1000.3, results.Result["Make"].Values.First(x => x.Range == "hyundai").Average.Value);
                }
            }
        }

        [Fact]
        public void CanGetAggregationQueryString()
        {
            using (var store = GetDocumentStore())
            {
                CreateAggregationSampleData(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Car>("Cars")
                        .AggregateBy(f => f.ByField(x => x.Make).SumOn(x => x.Price)).ToString();
                    Assert.Equal("from index 'Cars' select facet(Make, sum(Price))", query);
                }
            }
        }

        [Fact]
        public void CanGetAggregationQueryString_Async()
        {
            using (var store = GetDocumentStore())
            {
                CreateAggregationSampleData(store);

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Car>("Cars")
                        .AggregateBy(f => f.ByField(x => x.Make).SumOn(x => x.Price)).ToString();
                    Assert.Equal("from index 'Cars' select facet(Make, sum(Price))", query);
                }
            }
        }

        private void CreateAggregationSampleData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Car { Make = "Toyota", Year = 2011, Price = 1000.3m });
                session.Store(new Car { Make = "Toyota", Year = 2011, Price = 1400.3m });
                session.Store(new Car { Make = "Toyota", Year = 2012, Price = 2000.3m });
                session.Store(new Car { Make = "Ford", Year = 2011, Price = 900.3m });
                session.Store(new Car { Make = "Ford", Year = 2011, Price = 4900.3m });
                session.Store(new Car { Make = "Hyundai", Year = 2011, Price = 1000.3m });
                session.SaveChanges();
            }

            store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
            {
                Maps = { "from car in docs.Cars select new { car.Make, car.Year, car.Price}" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        "Price", new IndexFieldOptions
                        {
                            Storage = FieldStorage.Yes,
                        }
                    }
                },
                Name = "Cars" }
            }));

            Indexes.WaitForIndexing(store);
        }
    }
}
