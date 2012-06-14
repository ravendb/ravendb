using System;
using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class ComplexLinq : LocalClientTest
	{
		[Fact]
		public void QueryOnNegation()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					string first = "Ayende";
					string last = "Rahien";
					DateTime birthDate = new DateTime(2001, 1, 1);
					string country = "Israel";
					var queryable = session.Query<Client>().Where(x =>
																  x.FirstName != first &&
																  x.LastName == last &&
																  x.BirthDate == birthDate &&
																  x.Country == country);
					Assert.Equal("(((-FirstName:Ayende AND FirstName:*) AND LastName:Rahien) AND BirthDate:2001-01-01T00:00:00.0000000) AND Country:Israel", queryable.ToString());
					queryable.Any();

				}
			}
		}

		[Fact]
		public void QueryOnMultipleItems()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					string first = "Ayende";
					string last = "Rahien";
					DateTime birthDate = new DateTime(2001,1,1);
					string country = "Israel";
					var queryable = session.Query<Client>().Where(x =>
					                                              x.FirstName == first &&
					                                              x.LastName == last &&
					                                              x.BirthDate == birthDate &&
					                                              x.Country == country);
					Assert.Equal("((FirstName:Ayende AND LastName:Rahien) AND BirthDate:2001-01-01T00:00:00.0000000) AND Country:Israel", queryable.ToString());
					queryable.Any();

				}
			}
		}

		public class Client
		{
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public DateTime BirthDate { get; set; }
			public string Country { get; set; }
		}
	}
}
