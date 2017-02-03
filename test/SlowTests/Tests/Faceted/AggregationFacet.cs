// -----------------------------------------------------------------------
//  <copyright file="AggregationFacet.cs" company="Hibernating Rhinos LTD"> 
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class AggregationFacet : RavenNewTestBase
    {
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
                           .ToFacets(new[]
                           {
                               new Facet
                               {
                                   Aggregation = FacetAggregation.Max,
                                   AggregationField = "Price",
                                   Name = "Make"
                               }
                           });

                    Assert.Equal(3, results.Results["Make"].Values.Count);
                    Assert.Equal(1400.3, results.Results["Make"].Values.First(x => x.Range == "toyota").Max.Value);
                    Assert.Equal(4900.3, results.Results["Make"].Values.First(x => x.Range == "ford").Max.Value);
                    Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "hunday").Max.Value);
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
                                         .AggregateBy(x => x.Make)
                                         .CountOn(car => car.Price)
                                         .ToList();


                    Assert.Equal(3, results.Results["Make"].Values.Count);
                    Assert.Equal(2, results.Results["Make"].Values.First(x => x.Range == "toyota").Count.Value);
                    Assert.Equal(2, results.Results["Make"].Values.First(x => x.Range == "ford").Count.Value);
                    Assert.Equal(1, results.Results["Make"].Values.First(x => x.Range == "hunday").Count.Value);
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
                                         .AggregateBy(x => x.Make)
                                         .MaxOn(car => car.Price)
                                         .ToList();


                    Assert.Equal(3, results.Results["Make"].Values.Count);
                    Assert.Equal(1400.3, results.Results["Make"].Values.First(x => x.Range == "toyota").Max.Value);
                    Assert.Equal(4900.3, results.Results["Make"].Values.First(x => x.Range == "ford").Max.Value);
                    Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "hunday").Max.Value);
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
                                         .AggregateBy(x => x.Make)
                                         .MinOn(car => car.Price)
                                         .ToList();

                    Assert.Equal(3, results.Results["Make"].Values.Count);
                    Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "toyota").Min.Value);
                    Assert.Equal(900.3, results.Results["Make"].Values.First(x => x.Range == "ford").Min.Value);
                    Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "hunday").Min.Value);
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
                                         .AggregateBy(x => x.Make)
                                         .SumOn(car => car.Price)
                                         .ToList();


                    Assert.Equal(3, results.Results["Make"].Values.Count);
                    Assert.Equal(2400.6, results.Results["Make"].Values.First(x => x.Range == "toyota").Sum.Value);
                    Assert.Equal(5800.6, results.Results["Make"].Values.First(x => x.Range == "ford").Sum.Value);
                    Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "hunday").Sum.Value);
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
                                         .AggregateBy(x => x.Make)
                                         .AverageOn(car => car.Price)
                                         .ToList();


                    Assert.Equal(3, results.Results["Make"].Values.Count);
                    Assert.Equal(2400.6 / 2, results.Results["Make"].Values.First(x => x.Range == "toyota").Average.Value);
                    Assert.Equal(5800.6 / 2, results.Results["Make"].Values.First(x => x.Range == "ford").Average.Value);
                    Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "hunday").Average.Value);
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
                                         .AggregateBy(x => x.Make)
                                         .AverageOn(car => car.Price)
                                         .ToListAsync();


                    Assert.Equal(3, results.Result.Results["Make"].Values.Count);
                    Assert.Equal(2400.6 / 2, results.Result.Results["Make"].Values.First(x => x.Range == "toyota").Average.Value);
                    Assert.Equal(5800.6 / 2, results.Result.Results["Make"].Values.First(x => x.Range == "ford").Average.Value);
                    Assert.Equal(1000.3, results.Result.Results["Make"].Values.First(x => x.Range == "hunday").Average.Value);
                }
            }
        }

        private static void CreateAggregationSampleData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Car { Make = "Toyota", Year = 2011, Price = 1000.3m });
                session.Store(new Car { Make = "Toyota", Year = 2011, Price = 1400.3m });
                session.Store(new Car { Make = "Toyota", Year = 2012, Price = 2000.3m });
                session.Store(new Car { Make = "Ford", Year = 2011, Price = 900.3m });
                session.Store(new Car { Make = "Ford", Year = 2011, Price = 4900.3m });
                session.Store(new Car { Make = "Hunday", Year = 2011, Price = 1000.3m });
                session.SaveChanges();
            }

            store.Admin.Send(new PutIndexOperation("Cars", new IndexDefinition
            {
                Maps = { "from car in docs.Cars select new { car.Make, car.Year, car.Price}" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        "Price", new IndexFieldOptions
                        {
                            Storage = FieldStorage.Yes,
                            Sort = SortOptions.NumericDouble
                        }
                    }
                }
            }));

            WaitForIndexing(store);
        }
    }
}
