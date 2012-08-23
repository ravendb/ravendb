using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class IndexSelectionForMapReduce : RavenTest
	{
		[Fact]
		public void TestIndexSelectionForMapReduce()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					CreateTestData(session);

					RavenQueryStatistics stats1;
					RavenQueryStatistics stats2;
					RavenQueryStatistics stats3;

					// We expect each product to come back as they all have category-1 assigned
					session.Query<Product>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(d=>d.Categories.Any(e=>e.Id == "category-1"))
						.Statistics(out stats1)
						.ToList();
					Assert.Equal(10, stats1.TotalResults);
					

					// We expect each product to come back as there is no filter. Initial failing test is using the index created by the first query and returning a cartesian product
					session.Query<Product>()
						.Customize(x => x.WaitForNonStaleResults())
						.Statistics(out stats2)
						.ToList();
					Assert.Equal(10, stats2.TotalResults);
					Assert.NotEqual(stats1.IndexName, stats2.IndexName);

					// We expect a second query similar to the original to re-use the index
					session.Query<Product>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(d => d.Categories.Any(e => e.Id == "category-2"))
						.Statistics(out stats3)
						.ToList();
					Assert.Equal(10, stats3.TotalResults);
					Assert.Equal(stats1.IndexName, stats3.IndexName);
				}
			}
		}

		private static void CreateTestData(IDocumentSession session)
		{
			var categories = Enumerable.Range(1, 5).Select(j => new Category { Id = "category-" + j }).ToList();
			for (var i = 1; i <= 10; i++)
			{
				var product = new Product {
					Id = "product-" + i,
					Categories = categories
				};

				session.Store(product);
			}

			session.SaveChanges();
		}
	}

	public class Product
	{
		public string Id { get; set; }
		public IEnumerable<Category> Categories { get; set; }
	}

	public class Category
	{
		public string Id { get; set; }
	}
}
