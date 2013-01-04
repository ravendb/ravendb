using System.Collections.Generic;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SelectManyTests : RavenTest
	{
		[Fact]
		public void FindArticleByCategoryId()
		{
			using (var store = NewDocumentStore())
			{
				PopulateData(store);
				using (var session = store.OpenSession())
				{
					var query = session.Advanced.LuceneQuery<Product, ProductByCategoryIndex>()
						.WaitForNonStaleResults()
						.WhereEquals("CatIds", "categories/1")
						.AndAlso()
						.WhereEquals("CatIds", "categories/2");
					var result = query.ToArray();
					Assert.Equal(1, result.Length);
				}
			}
		}

		[Fact]
		public void FindArticleByCategoryName()
		{
			using (var store = NewDocumentStore())
			{
				PopulateData(store);
				using (var session = store.OpenSession())
				{
					var query = session.Advanced.LuceneQuery<Product, ProductByCategoryIndex>()
						.WaitForNonStaleResults()
						.WhereEquals("CatNames", "Cat2")
						.AndAlso()
						.WhereEquals("CatNames", "Cat1");
					var result = query.ToArray();
					Assert.Equal(1, result.Length);
				}
			}
		}

		void PopulateData(EmbeddableDocumentStore store)
		{
			new ProductByCategoryIndex().Execute(store);
			using (var session = store.OpenSession())
			{
				session.Store(new Category { Id = "categories/1", Name = "Cat1" });
				session.Store(new Category { Id = "categories/2", Name = "Cat2" });
				session.Store(new Product
				{
					Name = "Test1",
					Categories = new List<Category>
													   {
														   new Category { Id = "categories/1", Name = "Cat1" },
														   new Category { Id = "categories/2", Name = "Cat2" }
													   }
				});
				session.Store(new Product
				{
					Name = "Test2",
					Categories = new List<Category>
													   {
														   new Category { Id = "categories/2", Name = "Cat2" }
													   }
				});
				session.SaveChanges();
			}
			WaitForIndexing(store);
		}

		public class Product
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public int Value1 { get; set; }
			public int Value2 { get; set; }
			public List<Category> Categories { get; set; }
		}

		public class Category
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class ProductByCategoryIndex : AbstractIndexCreationTask<Product, Product>
		{
			public ProductByCategoryIndex()
			{
				Map = docs => from doc in docs
							  select new
							  {
								  Name = doc.Name,
								  CatIds = doc.Categories.Select(x => x.Id),
								  CatNames = doc.Categories.Select(x => x.Name),
							  };
			}
		}

	}
}
