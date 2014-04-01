using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_483 : NoDisposalNeeded
	{
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

		[Fact]
		public void WillNotForgetCastToNullableDateTime()
		{
			var indexDefinition = new IndexDefinitionBuilder<Person>
			{
				Map = persons => from p in persons select new {DateTime = (DateTime?) null}
			}.ToIndexDefinition(new DocumentConvention());

			const string expected = @"docs.People.Select(p => new {
    DateTime = ((DateTime ? ) null)
})";
			Assert.Equal(expected, indexDefinition.Map);
		}
	}
}