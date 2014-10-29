using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Support;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class FacetsHits : RavenTest
	{
		public class Product
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Category { get; set; }
			public Price Price { get; set; }
		}

		public class Price
		{
			public float Amount { get; set; }
			public string Currency { get; set; }
		}


		public class Products_Stats : AbstractIndexCreationTask<Product>
		{
			public Products_Stats()
			{
				Map = products =>
				      from product in products
				      select new
				      {
					      Category = product.Category,
					      Price = product.Price.Amount

				      };

                Sort(x=>x.Price, SortOptions.Float);
			}
		}

		[Fact]
		public void CanSearchOnAllProperties()
		{
			using (var store = NewDocumentStore())
			{

				new Products_Stats().Execute(store);

				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 1000; i++)
					{
						var amount = i % 50 / 10f;
						session.Store(
							new Product
							{
								Category = i % 2 == 0 ? "Cat1" : "Cat2",
								Name = "Product " + i,
								Price = new Price { Currency = "USD", Amount = amount }
							});
					}

					var facetSetup = new FacetSetup
					{
						Id = "facets/StatsFacet",
						Facets = new List<Facet>
						{
							new Facet
							{
								Name = "Category"
							},
							new Facet
							{
								Name = "Price_Range",
								Mode = FacetMode.Ranges,
								Ranges = new EquatableList<string>
								{
									"[NULL TO Fx0]",
									"[Fx0.001 TO Fx0.999]",
									"[Fx0.999 TO Fx1.999]",
									"[Fx1.999 TO NULL]"
								}
							}
						}
					};
					session.Store(facetSetup);
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var s = store.OpenSession())
				{
					var query = s.Query<Product>("Products/Stats");
					var facetResults = query.ToFacets("facets/StatsFacet");

					var priceFacet = facetResults.Results["Price_Range"];

					foreach (var val in priceFacet.Values)
						Assert.NotEqual(0, val.Hits);
				}
			}
		}
	}
}