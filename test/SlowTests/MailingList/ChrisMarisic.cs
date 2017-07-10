using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Transformers;
using Raven.Client.Json;
using Sparrow.Json;
using Xunit;

namespace SlowTests.MailingList
{
    public class ChrisMarisic : RavenTestBase
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

        [Fact]
        public void Physical_store_test()
        {
            List<Car> cars;
            using (var documentStore = GetDocumentStore())
            {
                documentStore.Initialize();

                new CarIndex().Execute(documentStore);
                new CarTransformer().Execute(documentStore);

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
                    var indexQuery = new IndexQuery() { Query = query.ToString(), PageSize = 1024, };

                    using (var commands = documentStore.Commands())
                    {
                        var queryResult = commands.Query(indexQuery);

                        var carLots = queryResult
                            .Results
                            .Select(x =>
                            {
                                using (var reader = new BlittableJsonReader())
                                {
                                    reader.Init((BlittableJsonReaderObject)x);
                                    return deserializer.Deserialize<CarLot>(reader);
                                }
                            })
                            .ToArray();

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
            }

            Assert.NotNull(cars);
            Assert.NotEmpty(cars);

            foreach (Car car in cars)
            {
                Assert.NotNull(car.LeaseHistory);
                Assert.NotEmpty(car.LeaseHistory);
            }
        }

        private class CarLot
        {
            public IList<Car> Cars { get; set; }
            public string Id { get; set; }
            public string Name { get; set; }
        }


        private class Car
        {
            public IList<Leasee> LeaseHistory { get; set; }
            public string Make { get; set; }
            public string Model { get; set; }
        }

        private class Leasee
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class CarIndex : AbstractIndexCreationTask<CarLot, CarIndex.IndexResult>
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


                Store(x => x.LotName, FieldStorage.Yes);
                Store(x => x.Make, FieldStorage.Yes);
                Store(x => x.Model, FieldStorage.Yes);
            }

            public class IndexResult
            {
                public string Bar { get; set; }
                public string Foo { get; set; }
                public string Id { get; set; }
                public string LotName { get; set; }
                public string Make { get; set; }
                public string Model { get; set; }
            }
        }

        private class CarTransformer : AbstractTransformerCreationTask<CarIndex.IndexResult>
        {
            public CarTransformer()
            {
                TransformResults = results =>
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
            }
        }
    }
}
