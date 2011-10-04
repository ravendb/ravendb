using System.Collections.Generic;
using Raven.Abstractions.Indexing;
using Xunit;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class FullTextSearchOnTags : RavenTest
	{
		public class Image
		{
			public string Id { get; set; }
			public ICollection<string> Tags { get; set; }
		}

		[Fact]
		public void CanSearchUsingPhrase()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Image
					{
						Tags = new []{ "cats", "animal", "feline"}
					});
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs.Images select new { doc.Tags }",
				});
				
				using (var session = store.OpenSession())
				{
					var images = session.Query<Image>("test")
						.Customize(x=>x.WaitForNonStaleResults())
						.Search(x => x.Tags, "i love cats")
						.ToList();
					Assert.NotEmpty(images);
				}
			}
		}
	}
}