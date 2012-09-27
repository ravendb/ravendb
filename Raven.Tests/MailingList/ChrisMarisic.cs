using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class ChrisMarisic : RavenTest
	{
		private static readonly Leasee[] CarLeasees = new[]
		{
			new Leasee {Id = "leasees/1", Name = "Bob"}, new Leasee {Id = "leasees/2", Name = "Steve"},
			new Leasee {Id = "leasees/3", Name = "Three"}
		};

		private static readonly CarLot[] Docs = new[]
		{
			new CarLot
			{
				Name = "Highway",
				Cars = new List<Car>
				{
					new Car {Make = "Ford", Model = "Explorer", LeaseHistory = new List<Leasee>(CarLeasees.Take(2))},
					new Car {Make = "Honda", Model = "Civic", LeaseHistory = new List<Leasee>(CarLeasees.Skip(1).Take(2))},
					new Car {Make = "Chevy", Model = "Cavalier", LeaseHistory = new List<Leasee> {CarLeasees[2]}},
				},
			},
			new CarLot
			{
				Name = "Airport",
				Cars = new List<Car>
				{
					new Car {Make = "Rolls Royce", Model = "Phantom", LeaseHistory = new List<Leasee>(CarLeasees.Take(2))},
					new Car {Make = "Cadillac", Model = "Escalade", LeaseHistory = new List<Leasee>(CarLeasees.Skip(1).Take(2))},
					new Car {Make = "GMC", Model = "Yukon", LeaseHistory = new List<Leasee> {CarLeasees[2]}},
				},
			}
		};

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.TempIndexPromotionMinimumQueryCount = 1;
		}

		[Fact]
		public void Physical_store_test()
		{
			List<Car> cars;
			using (var documentStore = NewRemoteDocumentStore())
			{
				documentStore.Initialize();

				new CarIndex().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					foreach (CarLot doc in Docs)
					{
						session.Store(doc);
					}
					session.SaveChanges();
				}

				string targetLeasee = CarLeasees[0].Id;

				using (var session = documentStore.OpenSession())
				{
					//Synchronize indexes
					session.Query<CarLot, CarIndex>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(2))).FirstOrDefault();

					var query = session.Query<CarLot, CarIndex>()
						.Where(carLot => carLot.Cars.Any(car => car.LeaseHistory.Any(leasee => leasee.Id == targetLeasee)))
						.Take(1024);

					var deserializer = session.Advanced.DocumentStore.Conventions.CreateSerializer();
					var indexQuery = new IndexQuery { Query = query.ToString(), SkipTransformResults = true, PageSize = 1024, };

					var queryResult = session.Advanced.DocumentStore.DatabaseCommands.Query(typeof(CarIndex).Name, indexQuery, new string[0]);

					var carLots = queryResult
						.Results
						.Select(x => deserializer.Deserialize<CarLot>(new RavenJTokenReader(x)))
						.ToArray()
						;

					foreach (var carLot in carLots)
					{
						Assert.NotNull(carLot.Cars);
						Assert.NotEmpty(carLot.Cars);
					}

					cars = carLots
						.SelectMany(x => x.Cars)
						.Where(car => car.LeaseHistory.Any(leasee => leasee.Id == targetLeasee))
						.ToList();
				}
			}

			Assert.NotNull(cars);
			Assert.NotEmpty(cars);

			foreach (Car car in cars)
			{
				Assert.NotNull(car.LeaseHistory);
				Assert.NotEmpty(car.LeaseHistory);
			}
		}

		[Fact]
		public void Embedded_store_test()
		{
			List<Car> cars;
			using (
				var documentStore = new EmbeddableDocumentStore
				{Configuration = {RunInMemory = true, RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true}})
			{
				documentStore.Initialize();

				new CarIndex().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					foreach (CarLot doc in Docs)
					{
						session.Store(doc);
					}
					session.SaveChanges();
				}

				string targetLeasee = CarLeasees[0].Id;

				using (var session = documentStore.OpenSession())
				{
					//Synchronize indexes
					session.Query<CarLot, CarIndex>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(2))).FirstOrDefault();

					var query = session.Query<CarLot, CarIndex>()
						.Where(carLot => carLot.Cars.Any(car => car.LeaseHistory.Any(leasee => leasee.Id == targetLeasee)))
						.Take(1024);

					var deserializer = session.Advanced.DocumentStore.Conventions.CreateSerializer();
					var indexQuery = new IndexQuery {Query = query.ToString(), SkipTransformResults = true, PageSize = 1024,};

					var queryResult = session.Advanced.DocumentStore.DatabaseCommands.Query(typeof (CarIndex).Name, indexQuery,
					                                                                        new string[0]);

					var carLots = queryResult
						.Results
						.Select(x => deserializer.Deserialize<CarLot>(new RavenJTokenReader(x)))
						.ToArray()
						;

					foreach (var carLot in carLots)
					{
						Assert.NotNull(carLot.Cars);
						Assert.NotEmpty(carLot.Cars);
					}

					cars = carLots
						.SelectMany(x => x.Cars)
						.Where(car => car.LeaseHistory.Any(leasee => leasee.Id == targetLeasee))
						.ToList();
				}
			}

			Assert.NotNull(cars);
			Assert.NotEmpty(cars);

			foreach (Car car in cars)
			{
				Assert.NotNull(car.LeaseHistory);
				Assert.NotEmpty(car.LeaseHistory);
			}
		}

		public class CarLot
		{
			public IList<Car> Cars { get; set; }
			public string Id { get; set; }
			public string Name { get; set; }
		}


		public class Car
		{
			public IList<Leasee> LeaseHistory { get; set; }
			public string Make { get; set; }
			public string Model { get; set; }
		}

		public class Leasee
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class CarIndex : AbstractIndexCreationTask<CarLot, CarIndex.IndexResult>
		{
			public CarIndex()
			{
				Map = carLots => from carLot in carLots
				                 from car in carLot.Cars
				                 select new
				                 {
					                 carLot.Id,
					                 LotName = carLot.Name,
					                 car.Make,
					                 car.Model,
					                 Cars_LeaseHistory_Id = car.LeaseHistory.Select(x => x.Id),
					                 Cars_LeaseHistory_Name = car.LeaseHistory.Select(x => x.Name)
				                 };


				TransformResults = (db, results) =>
				                   from result in results
				                   select new
				                   {
					                   result.Id,
					                   result.LotName,
					                   result.Make,
					                   result.Model,
					                   Foo = "bar",
					                   Bar = "foo",
				                   };

				Store(x => x.LotName, FieldStorage.Yes);
				Store(x => x.Make, FieldStorage.Yes);
				Store(x => x.Model, FieldStorage.Yes);
			}

			#region Nested type: IndexResult

			public class IndexResult
			{
				public string Bar { get; set; }
				public string Foo { get; set; }
				public string Id { get; set; }
				public string LotName { get; set; }
				public string Make { get; set; }
				public string Model { get; set; }
			}

			#endregion
		}
	}
}