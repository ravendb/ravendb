using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class DanTurner : RavenTestBase
    {
        public DanTurner(ITestOutputHelper output) : base(output)
        {
        }

        private Person _john;
        private Car _patrol;
        private Car _focus;

        private Person _mary;
        private Car _falcon;
        private Car _astra;

        private void CreateData(IDocumentStore store)
        {
            new DriversIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                _patrol = new Car("AAA-000", "Nissan", "Patrol");
                _focus = new Car("BBB-111", "Ford", "Focus");
                _john = new Person("John Smith");
                _john.Drives(_patrol);
                _john.Drives(_focus);

                session.Store(_john);

                _falcon = new Car("CCC-222", "Ford", "Falcon");
                _astra = new Car("DDD-333", "Holden", "Astra");
                _mary = new Person("Mary Smith");
                _mary.Drives(_falcon);
                _mary.Drives(_astra);

                session.Store(_mary);

                session.SaveChanges();
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanEnumerateQueryOnDriversIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<Person, DriversIndex>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .ProjectInto<Driver>()
                        .ToList();

                    Assert.Equal(4, results.Count);

                    Assert.True(ContainsSingleMatch(results, _john, _patrol));
                    Assert.True(ContainsSingleMatch(results, _john, _focus));
                    Assert.True(ContainsSingleMatch(results, _mary, _falcon));
                    Assert.True(ContainsSingleMatch(results, _mary, _astra));
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanEnumerateDummiedMapResult(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CreateData(store);

                var persons = new[] {_john, _mary};
                var results = (
                    from person in persons
                    from car in person.Cars
                    select new Driver()
                    {
                        PersonId = person.Id,
                        PersonName = person.Name,
                        CarRegistration = car.Registration,
                        CarMake = car.Make
                    }
                ).ToList();

                Assert.Equal(4, results.Count);

                Assert.True(ContainsSingleMatch(results, _john, _patrol));
                Assert.True(ContainsSingleMatch(results, _john, _focus));
                Assert.True(ContainsSingleMatch(results, _mary, _falcon));
                Assert.True(ContainsSingleMatch(results, _mary, _astra));
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanCountByQueryOnDriversIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Query<Driver, DriversIndex>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .Select(x => new {x.PersonId, x.PersonName, x.CarRegistration, x.CarMake});

                    Assert.Equal(4, results.Count());
                }
            }
        }

        private bool ContainsSingleMatch(IEnumerable<Driver> drivers, Person person, Car car)
        {
            var matches = drivers.Count(x =>
                String.Equals(x.PersonId, person.Id) &&
                String.Equals(x.PersonName, person.Name) &&
                String.Equals(x.CarRegistration, car.Registration) &&
                String.Equals(x.CarMake, car.Make)
            );

            return matches == 1;
        }

        private class DriversIndex : AbstractIndexCreationTask<Person, Driver>
        {
            public DriversIndex()
            {
                Map = persons => from person in persons
                                 from car in person.Cars
                                 select new
                                 {
                                     PersonId = person.Id,
                                     PersonName = person.Name,
                                     CarRegistration = car.Registration,
                                     CarMake = car.Make
                                 };

                Store(p => p.PersonId, FieldStorage.Yes);
                Store(p => p.PersonName, FieldStorage.Yes);
                Store(p => p.CarRegistration, FieldStorage.Yes);
                Store(p => p.CarMake, FieldStorage.Yes);
            }
        }


        private class Driver
        {
            public string PersonId { get; set; }

            public string PersonName { get; set; }

            public string CarRegistration { get; set; }

            public string CarMake { get; set; }
        }

        private class Car
        {
            public Car(string registration, string make, string model)
            {
                Registration = registration;
                Make = make;
                Model = model;
            }

            public string Registration { get; set; }
            public string Make { get; set; }
            public string Model { get; set; }
        } 

        private class Person
        {
            public Person(string name)
            {
                Id = "persons/";
                Cars = new List<Car>();
                Name = name;
            }

            public string Id { get; set; }
            public string Name { get; set; }
            public List<Car> Cars { get; set; }

            public void Drives(Car car)
            {
                Cars.Add(car);
            }
        } 
    }
}
