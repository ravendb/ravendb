using System;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB903 : RavenTestBase
	{
		public class Product
		{
			public string Name { get; set; }
			public string Description { get; set; }
		}

		[Fact]
		public void Test1()
		{
			DoTest(session => session.Query<Product, TestIndex>()
									 .Search(x => x.Description, "Hello")
									 .Intersect()
									 .Where(x => x.Name == "Bar")
									 .As<Product>());
		}

		[Fact]
		public void Test2()
		{
			DoTest(session => session.Query<Product, TestIndex>()
									 .Where(x => x.Name == "Bar")
									 .Intersect()
									 .Search(x => x.Description, "Hello")
									 .As<Product>());
		}

		public void DoTest(Func<IDocumentSession, IQueryable<Product>> queryFunc)
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new TestIndex());

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Product { Name = "Foo", Description = "Hello World" });
					session.Store(new Product { Name = "Bar", Description = "Hello World" });
					session.Store(new Product { Name = "Bar", Description = "Goodbye World" });

					session.SaveChanges();
				}

				WaitForIndexing(documentStore);

				using (var session = documentStore.OpenSession())
				{
					var query = queryFunc(session);

					Debug.WriteLine(query);
					Debug.WriteLine("");

					var products = query.ToList();
					foreach (var product in products)
					{
						Debug.WriteLine(JsonConvert.SerializeObject(product, Formatting.Indented));
					}

					Assert.Equal(1, products.Count);
				}
			}
		}

		public class TestIndex : AbstractIndexCreationTask<Product>
		{
			public TestIndex()
			{
				Map = products => from product in products
								  select new
								  {
									  product.Name,
									  product.Description
								  };

				Index(x => x.Description, FieldIndexing.Analyzed);
			}
		}
	}
}