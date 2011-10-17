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
			public ICollection<string> Users { get; set; }
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


		[Fact]
		public void MultipleSearches()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Image
					{
						Tags = new[] { "cats", "animal", "feline" },
						Users = new[]{"oren", "ayende"}
					});
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs.Images select new { doc.Tags, doc.Users }",
				});

				using (var session = store.OpenSession())
				{
					var query = session.Query<Image>("test")
						.Customize(x => x.WaitForNonStaleResults())
						.Search(x => x.Tags, "i love cats")
						.Search(x=>x.Users, "oren")
						.ToString();
					Assert.Equal("Tags:<<i love cats>>  Users:<<oren>>", query.Trim());
				}
			}
		}

		[Fact]
		public void UsingSuggest()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Image
					{
						Tags = new[] { "cats", "animal", "feline" },
						Users = new[] { "oren", "ayende" }
					});
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs.Images select new { doc.Tags, doc.Users }",
					Indexes = {{"Tags", FieldIndexing.Analyzed}}
				});

				using (var session = store.OpenSession())
				{
					session.Query<Image>("test")
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();

					var query = session.Query<Image>("test")
						.Search(x => x.Tags, "anmal lover")
						.Suggest();
					Assert.NotEmpty(query.Suggestions);
					Assert.Equal("animal", query.Suggestions[0]);
				}
			}
		}
	}
}