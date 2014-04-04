using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB982 : RavenTest
	{
		[Fact]
		public void WillNotForceValuesToBeString()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Person { Age = 4 });
					session.Store(new Person { Age = 4 });
					session.SaveChanges();
				}

				new PeopleByAge().Execute(store);

				WaitForIndexing(store);

				var queryResult = store.DatabaseCommands.Query("PeopleByAge", new IndexQuery(), null);
				Assert.Equal(JTokenType.Integer, queryResult.Results[0]["Age"].Type);
				Assert.Equal(JTokenType.Integer, queryResult.Results[0]["Count"].Type);
			}
		}

		public class PeopleByAge : AbstractIndexCreationTask<Person, PeopleByAge.Result>
		{
			public class Result
			{
				public int Age { get; set; }
				public int Count { get; set; }
			}

			public PeopleByAge()
			{
				Map = persons =>
				      from person in persons
				      select new
				      {
					      Count = 1,
					      person.Age
				      };
				Reduce = results =>
				         from result in results
				         group result by result.Age
				         into g
				         select new
				         {
							 Count = g.Sum(x=>x.Count),
							 Age = g.Key
				         };
			}
		}

		public class Person
		{
			public int Age { get; set; }
		}
	}
}