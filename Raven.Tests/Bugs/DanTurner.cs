using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs
{
	public class DanTurner : LocalClientTest, IDisposable
	{
		private IDocumentStore _store;

		private Person _john;
		private Car _patrol;
		private Car _focus;

		private Person _mary;
		private Car _falcon;
		private Car _astra;

		public DanTurner()
		{
			_store = NewDocumentStore();


			new DriversIndex().Execute(_store);

			using (var session = _store.OpenSession())
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

		public override void Dispose()
		{
			_store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanEnumerateQueryOnDriversIndex()
		{
			using (var session = _store.OpenSession())
			{
				var results = session
					.Query<Person, DriversIndex>()
					.Customize(c => c.WaitForNonStaleResults())
					.AsProjection<Driver>()
					.ToList();

				Assert.Equal(4, results.Count);

				Assert.True(ContainsSingleMatch(results, _john, _patrol));
				Assert.True(ContainsSingleMatch(results, _john, _focus));
				Assert.True(ContainsSingleMatch(results, _mary, _falcon));
				Assert.True(ContainsSingleMatch(results, _mary, _astra));
			}
		}

		[Fact]
		public void CanEnumerateDummiedMapResult()
		{
			var persons = new[] { _john, _mary };
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
		
		[Fact]
		public void CanCountByQueryOnDriversIndex()
		{
			using (var session = _store.OpenSession())
			{
				var results = session
					.Query<Driver, DriversIndex>()
					.Customize(c => c.WaitForNonStaleResults())
					.Select(x=>new{x.PersonId, x.PersonName, x.CarRegistration, x.CarMake});

				Assert.Equal(4, results.Count());
			}
		}


		public class DriversIndex : AbstractIndexCreationTask<Person, Driver>
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

				Sort(p => p.CarMake, Raven.Abstractions.Indexing.SortOptions.String);
				Store(p => p.PersonId, FieldStorage.Yes);
				Store(p => p.PersonName, FieldStorage.Yes);
				Store(p => p.CarRegistration, FieldStorage.Yes);
				Store(p => p.CarMake, FieldStorage.Yes);
			}
		}


		public class Driver
		{
			public string PersonId { get; set; }

			public string PersonName { get; set; }

			public string CarRegistration { get; set; }

			public string CarMake { get; set; }
		}
		public class Car
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

		public class Person
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