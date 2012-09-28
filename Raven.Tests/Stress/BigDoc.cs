using System;
using Raven.Client.Document;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Stress
{
	[CLSCompliant(false)]
	public class BigDoc : RavenTest
	{
		[Theory]
		[InlineValue(5600)]
		[InlineValue(11200)]
		[InlineValue(56000)]
		public void CanSaveBigDocwhenUsingEmbedded(int size)
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var products = new Products(size);
					for (int i = 0; i < size; i++)
					{
						products.ProductList[i] = new Product() { Cost = i, Id = i.ToString(), Name = "this is item " + i };
					}

					session.Store(products);
					session.SaveChanges();
				}
			}
		}

		[Theory]
		[InlineValue(5600)]
		[InlineValue(11200)]
		[InlineValue(56000)]
		public void CanSaveBigDocwhenUsingServer(int size)
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var products = new Products(size);
					for (int i = 0; i < size; i++)
					{
						products.ProductList[i] = new Product() { Cost = i, Id = i.ToString(), Name = "this is item " + i };
					}

					session.Store(products);
					session.SaveChanges();
				}
			}
		}


		public class Products
		{
			public Product[] ProductList { get; set; }
			public string Category { get; set; }
			public Products()
			{

			}

			public Products(int size)
			{
				ProductList = new Product[size];
			}
		}
		public class Product
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public decimal Cost { get; set; }
		}
	}
}