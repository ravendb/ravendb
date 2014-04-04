using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;
using System.Linq;
using Raven.Client;

namespace Raven.Tests.Bugs
{
	public class CanGetScores : RavenTest
	{
		[Fact]
		public void FromQuery()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new User{ Name = "who is knocking on my doors"});
					s.Store(new User { Name = "doors ltd" });
					s.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name}",
					Indexes = {{"Name", FieldIndexing.Analyzed}}
				});

				using (var s = store.OpenSession())
				{
					var users = s.Query<User>("test")
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Name == "doors")
						.ToList();
					Assert.Equal(2, users.Count);
					foreach (var user in users)
					{
						var score = s.Advanced.GetMetadataFor(user)["Temp-Index-Score"];
						Assert.NotNull(score);
					}
				}
			}
		}


		[Fact]
		public void FromQueryWithOrderByScoreThenName()
		{
			using (var store = NewDocumentStore ())
			{
				using (var s = store.OpenSession ())
				{
					s.Store (new User { Name = "who is knocking on my doors" });
					s.Store (new User { Name = "doors doors ltd" });
					s.Store (new User { Name = "doors doors abc" });
					s.SaveChanges ();
				}

				// Overloading the email property into a catchall freeform container to avoid rewriting the test entirely.
				store.DatabaseCommands.PutIndex ("test", new IndexDefinition
				{
					Map = "from doc in docs select new { Email = doc.Name, Name = doc.Name }",
					Indexes = { { "Email", FieldIndexing.Analyzed } }
				});

				using (var s = store.OpenSession ())
				{
					var users = s.Query<User> ("test")
						.Customize (x => x.WaitForNonStaleResults ())
						.Where (x => x.Email == "doors")
						.OrderByScore().ThenBy(x => x.Name)
						.ToList ();

					Assert.Equal (3, users.Count);

					var sorted = (from u in users
									let score = s.Advanced.GetMetadataFor (u).Value<double> ("Temp-Index-Score")
									orderby score descending, u.Name
								  select new { score, u.Name }).ToList ();

					for (int i = 0; i < users.Count; i++)
					{
						Assert.Equal (sorted[i].Name, users[i].Name);
						var score = s.Advanced.GetMetadataFor (users[i])["Temp-Index-Score"];
						Assert.NotNull (score);
					}
				}
			}
		}

		[Fact]
		public void FromQueryWithOrderByScoreThenNameDescending()
		{
			using (var store = NewDocumentStore ())
			{
				using (var s = store.OpenSession ())
				{
					s.Store (new User { Name = "who is knocking on my doors" });
					s.Store (new User { Name = "doors doors ltd" });
					s.Store (new User { Name = "doors doors abc" });
					s.SaveChanges ();
				}

				// Overloading the email property into a catchall freeform container to avoid rewriting the test entirely.
				store.DatabaseCommands.PutIndex ("test", new IndexDefinition
				{
					Map = "from doc in docs select new { Email = doc.Name, Name = doc.Name }",
					Indexes = { { "Email", FieldIndexing.Analyzed } }
				});

				using (var s = store.OpenSession ())
				{
					var users = s.Query<User> ("test")
						.Customize (x => x.WaitForNonStaleResults ())
						.Where (x => x.Email == "doors")
						.OrderByScore ().ThenByDescending(x => x.Name)
						.ToList ();

					Assert.Equal (3, users.Count);

					var sorted = (from u in users
								  let score = s.Advanced.GetMetadataFor (u).Value<double> ("Temp-Index-Score")
								  orderby score descending, u.Name descending
								  select new { score, u.Name }).ToList ();

					for (int i = 0; i < users.Count; i++)
					{
						Assert.Equal (sorted[i].Name, users[i].Name);
						var score = s.Advanced.GetMetadataFor (users[i])["Temp-Index-Score"];
						Assert.NotNull (score);
					}
				}
			}
		}
	}
}