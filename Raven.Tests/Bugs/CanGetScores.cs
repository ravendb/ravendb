using Raven.Abstractions.Indexing;
using Xunit;
using System.Linq;

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
	}
}
