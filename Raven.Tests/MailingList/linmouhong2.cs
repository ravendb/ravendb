using System;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class linmouhong2 : RavenTest
	{
		public class Product
		{
			public string Id { get; set; }

			public string Name { get; set; }

			public decimal Price { get; set; }

			public CategoryReference Category { get; set; }
		}

		public class CategoryReference
		{
			public int Id { get; set; }

			public string Name { get; set; }
		}

		public class ProductIndex : AbstractIndexCreationTask<Product>
		{
			public ProductIndex()
			{
				Map = products => from p in products
								  select new
								  {
									  p.Id,
									  p.Name,
									  Category_Id = p.Category.Id,
									  Category_Name = p.Category.Name
								  };

				// Sort(x => x.Category.Id, Raven.Abstractions.Indexing.SortOptions.Int);

				Index(x => x.Name, Raven.Abstractions.Indexing.FieldIndexing.Analyzed);
			}
		}
	 
		[Fact]
		public void CanQuerySuccessfully()
		{
			using(var database = NewDocumentStore())
			{
				new ProductIndex().Execute(database);
				using (var session = database.OpenSession())
				{
					session.Store(new Product
					{
						Name = "Product 1"
					});
					session.Store(new Product
					{
						Name = "Product 2"
					});
					session.SaveChanges();
				}

				using (var session = database.OpenSession())
				{
					var products = session.Query<Product, ProductIndex>()
											.Customize(x => x.WaitForNonStaleResultsAsOfNow())
											.OrderBy(x => x.Category.Id)
											.ToList();

					Assert.NotEmpty(products);
				}
			}
		}
	}
}