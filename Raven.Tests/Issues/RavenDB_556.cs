namespace Raven.Tests.Issues
{
	using System.Linq;

	using Raven.Abstractions.Data;
	using Raven.Client.Document;

	using Xunit;

	public class RavenDB_556 : RemoteClientTest
	{
		public class Person
		{
			public string FirstName { get; set; }

			public string LastName { get; set; }

			public string MiddleName { get; set; }
		}

		[Fact]
		public void IndexEntryFieldShouldNotContainNullValues()
		{
			using (var server = GetNewServer())
			{
				using (var docStore = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
				{
					using (var session = docStore.OpenSession())
					{
						session.Store(new Person { FirstName = "John", MiddleName = null, LastName = null });
						session.Store(new Person { FirstName = "William", MiddleName = "Edgard", LastName = "Smith" });
						session.Store(new Person { FirstName = "Paul", MiddleName = null, LastName = "Smith" });
						session.SaveChanges();
					}

					using (var session = docStore.OpenSession())
					{
						session.Query<Person>()
							.Customize(x => x.WaitForNonStaleResults())
							.Where(x => x.FirstName == "John" || x.FirstName == "Paul")
							.ToList();

						var indexes = session
							.Advanced
							.DocumentStore
							.DatabaseCommands
							.GetIndexNames(0, 100);

						var newIndexes = indexes
							.Where(x => x.StartsWith("Temp"));

						Assert.Equal(1, newIndexes.Count());

						var queryResult = session
							.Advanced
							.DocumentStore
							.DatabaseCommands
							.Query(newIndexes.First(), new IndexQuery(), null, false, true);

						foreach (var result in queryResult.Results)
						{
							var q = result["FirstName"].ToString();
							Assert.NotNull(q);
							Assert.True(new[] { "john", "william", "paul" }.Contains(q));
						}
					}
				}
			}
		}
	}
}