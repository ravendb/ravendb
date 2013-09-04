// -----------------------------------------------------------------------
//  <copyright file="AggregationFacet.cs" company="Hibernating Rhinos LTD"> 
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Xunit;
using Raven.Client;
using System.Linq;

namespace Raven.Tests.Faceted
{
	public class AggregationFacet : RavenTest
	{
		public class Car
		{
			public string Make { get; set; }
			public int Year { get; set; }
			public decimal Price { get; set; }
		}

		[Fact]
		public void CanHandleMaxFacet_LowLevel()
		{
			using (var store = NewDocumentStore())
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
					Assert.Equal(1400.3, results.Results["Make"].Values.First(x => x.Range == "toyota").Value);
					Assert.Equal(4900.3, results.Results["Make"].Values.First(x => x.Range == "ford").Value);
					Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "hunday").Value);
				}

			}
		}

		[Fact]
		public void CanHandleCountFacet_HighLevel()
		{
			using (var store = NewDocumentStore())
			{
				CreateAggregationSampleData(store);

				using (var session = store.OpenSession())
				{
					var results = session.Query<Car>("Cars")
										 .Where(car => car.Year == 2011)
										 .FacetOn(x => x.Make)
										 .CountOn(car => car.Price)
										 .ToList();


					Assert.Equal(3, results.Results["Make"].Values.Count);
					Assert.Equal(2, results.Results["Make"].Values.First(x => x.Range == "toyota").Value);
					Assert.Equal(2, results.Results["Make"].Values.First(x => x.Range == "ford").Value);
					Assert.Equal(1, results.Results["Make"].Values.First(x => x.Range == "hunday").Value);
				}
			}
		}

		[Fact]
		public void CanHandleMaxFacet_HighLevel()
		{
			using (var store = NewDocumentStore())
			{
				CreateAggregationSampleData(store);

				using (var session = store.OpenSession())
				{
					var results = session.Query<Car>("Cars")
					                     .Where(car => car.Year == 2011)
					                     .FacetOn(x => x.Make)
					                     .MaxOn(car => car.Price)
					                     .ToList();


					Assert.Equal(3, results.Results["Make"].Values.Count);
					Assert.Equal(1400.3, results.Results["Make"].Values.First(x => x.Range == "toyota").Value);
					Assert.Equal(4900.3, results.Results["Make"].Values.First(x => x.Range == "ford").Value);
					Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "hunday").Value);
				}
			}
		}

		[Fact]
		public void CanHandleMinFacet_HighLevel()
		{
			using (var store = NewDocumentStore())
			{
				CreateAggregationSampleData(store);

				using (var session = store.OpenSession())
				{
					var results = session.Query<Car>("Cars")
										 .Where(car => car.Year == 2011)
										 .FacetOn(x => x.Make)
										 .MinOn(car => car.Price)
										 .ToList();

					Assert.Equal(3, results.Results["Make"].Values.Count);
					Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "toyota").Value);
					Assert.Equal(900.3, results.Results["Make"].Values.First(x => x.Range == "ford").Value);
					Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "hunday").Value);
				}
			}
		}

		[Fact]
		public void CanHandleSumFacet_HighLevel()
		{
			using (var store = NewDocumentStore())
			{
				CreateAggregationSampleData(store);

				using (var session = store.OpenSession())
				{
					var results = session.Query<Car>("Cars")
										 .Where(car => car.Year == 2011)
										 .FacetOn(x => x.Make)
										 .SumOn(car => car.Price)
										 .ToList();


					Assert.Equal(3, results.Results["Make"].Values.Count);
					Assert.Equal(2400.6, results.Results["Make"].Values.First(x => x.Range == "toyota").Value);
					Assert.Equal(5800.6, results.Results["Make"].Values.First(x => x.Range == "ford").Value);
					Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "hunday").Value);
				}
			}
		}

		[Fact]
		public void CanHandleAverageFacet_HighLevel()
		{
			using (var store = NewDocumentStore())
			{
				CreateAggregationSampleData(store);

				using (var session = store.OpenSession())
				{
					var results = session.Query<Car>("Cars")
										 .Where(car => car.Year == 2011)
										 .FacetOn(x => x.Make)
										 .AverageOn(car => car.Price)
										 .ToList();


					Assert.Equal(3, results.Results["Make"].Values.Count);
					Assert.Equal(2400.6/2, results.Results["Make"].Values.First(x => x.Range == "toyota").Value);
					Assert.Equal(5800.6/2, results.Results["Make"].Values.First(x => x.Range == "ford").Value);
					Assert.Equal(1000.3, results.Results["Make"].Values.First(x => x.Range == "hunday").Value);
				}
			}
		}

		[Fact]
		public void CanHandleAverageFacetAsync_HighLevel()
		{
			using (var store = NewRemoteDocumentStore())
			{
				CreateAggregationSampleData(store);

				using (var session = store.OpenAsyncSession())
				{
					var results = session.Query<Car>("Cars")
										 .Where(car => car.Year == 2011)
										 .FacetOn(x => x.Make)
										 .AverageOn(car => car.Price)
										 .ToListAsync();


					Assert.Equal(3, results.Result.Results["Make"].Values.Count);
					Assert.Equal(2400.6 / 2, results.Result.Results["Make"].Values.First(x => x.Range == "toyota").Value);
					Assert.Equal(5800.6 / 2, results.Result.Results["Make"].Values.First(x => x.Range == "ford").Value);
					Assert.Equal(1000.3, results.Result.Results["Make"].Values.First(x => x.Range == "hunday").Value);
				}
			}
		}

		private static void CreateAggregationSampleData(IDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				session.Store(new Car {Make = "Toyota", Year = 2011, Price = 1000.3m});
				session.Store(new Car {Make = "Toyota", Year = 2011, Price = 1400.3m});
				session.Store(new Car {Make = "Toyota", Year = 2012, Price = 2000.3m});
				session.Store(new Car {Make = "Ford", Year = 2011, Price = 900.3m});
				session.Store(new Car {Make = "Ford", Year = 2011, Price = 4900.3m});
				session.Store(new Car {Make = "Hunday", Year = 2011, Price = 1000.3m});
				session.SaveChanges();
			}

			store.DatabaseCommands.PutIndex("Cars", new IndexDefinition
			{
				Map = "from car in docs.Cars select new { car.Make, car.Year, car.Price}",
				Stores = {{"Price", FieldStorage.Yes}}
			});

			WaitForIndexing(store);
		}
	}
}