using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class WithStartWith : RavenTest
	{
		[Fact]
		public void CanQueryDocumentsFilteredByMap()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
				                                new IndexDefinition
				                                {
				                                	Map =
				                                		"from doc in docs let Name = doc[\"@metadata\"][\"Name\"] where Name.StartsWith(\"Raven\") select new { Name }"
				                                });

				using(var s = store.OpenSession())
				{
					var entity = new {Name = "Ayende"};
					s.Store(entity);
					s.Advanced.GetMetadataFor(entity)["Name"] = "RavenDB";
					s.SaveChanges();
				}

				using(var s = store.OpenSession())
				{
					Assert.Equal(1, s.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).Count());
				}
			}
		}

	}
}