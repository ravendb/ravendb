using System.Collections.Generic;
using Lucene.Net.Analysis.Standard;
using Raven.Abstractions.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB_10 : RavenTest
	{
		public class Item
		{
			public string Text { get; set; }
			public int Age { get; set; }
		}

		[Fact]
		public void ShouldSortCorrectly()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item { Age = 10 });
					session.Store(new Item { Age = 3 });

					session.SaveChanges();
				}
				using(var session = store.OpenSession())
				{
					var items = session.Query<Item>()
						.Customize(x => x.WaitForNonStaleResults())
						.OrderBy(x => x.Age)
						.ToList();


					Assert.Equal(3, items[0].Age);
					Assert.Equal(10, items[1].Age);
				}
			}
		}

		[Fact]
		public void ShouldSearchCorrectly()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item { Text = "Seek's" });
					session.Store(new Item { Text = "Sit" });

					session.SaveChanges();
				}
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Text }",
					Analyzers = { { "Text", typeof(StandardAnalyzer).AssemblyQualifiedName } },
					Indexes = { { "Text", FieldIndexing.Analyzed } }
				});

				using (var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Query<Item>("test")
					                	.Customize(x => x.WaitForNonStaleResults())
										.Where(x => x.Text == "Seek")
					                	.ToList());

					Assert.NotEmpty(session.Query<Item>("test")
										.Customize(x => x.WaitForNonStaleResults())
										.Where(x => x.Text == "Sit's")
										.ToList());

				}
			}
		}
	}
}