using System.Linq;
using Raven.Abstractions.Indexing;
using Xunit;

namespace Raven.Tests.NestedIndexing
{
	public class CanIndexReferencedEntity : RavenTest
	{
		[Fact]
		public void Simple()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = @"
						from i in docs.Items
						select new
						{
							RefName = LoadDocument(i.Ref).Name,
						}"
				});

				using (var session = store.OpenSession())
				{
					session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
					session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var item = session.Advanced.LuceneQuery<Item>("test")
					                  .WaitForNonStaleResults()
					                  .WhereEquals("RefName", "ayende")
					                  .Single();
					Assert.Equal("items/1", item.Id);
				}
			}
		}
	}
}