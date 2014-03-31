using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class FacetQueryNotCorrectWithDefaultFieldSpecified : RavenTest
	{
		/// <summary>
		/// Works
		/// </summary>
		[Fact]
		public void ShouldWorkWithEmbeddedRaven()
		{
			//arrange
			using(var store = NewDocumentStore())
			{
				store.RegisterListener(new NoStaleQueriesListener());

				SetupTestData(store);

				WaitForIndexing(store);

				//Act
				FacetResults result = ExecuteTest(store);

				//Assert
				CheckResults(result);
			}
		}

		/// <summary>
		/// Should work but does not
		/// </summary>
		[Fact]
		public void ShouldWorkWithRavenServer()
		{
			//arrange
			using(var store = NewRemoteDocumentStore())
			{
				store.RegisterListener(new NoStaleQueriesListener());

				SetupTestData(store);

				WaitForIndexing(store);

				//Act
				FacetResults result = ExecuteTest(store);
                WaitForUserToContinueTheTest();
				//Assert
				CheckResults(result);
			}
		}

		private static void CheckResults(FacetResults result)
		{
			Assert.Contains("Brand", result.Results.Select(x=>x.Key));
			FacetResult facetResult = result.Results["Brand"];
			Assert.Equal(1, facetResult.Values.Count);
			facetResult.Values[0] = new FacetValue { Range = "mybrand1", Hits = 1 };
		}

		private static FacetResults ExecuteTest(IDocumentStore store)
		{
			FacetResults result = store.DatabaseCommands.GetFacets("Product/AvailableForSale2", new IndexQuery { Query = "MyName1", DefaultField = "Any" }, "facets/ProductFacets");
			return result;
		}

		private static void SetupTestData(IDocumentStore store)
		{
			new Product_AvailableForSale2().Execute(store);

			Product product1 = new Product("MyName1", "MyBrand1");
			Product product2 = new Product("MyName2", "MyBrand2");

			FacetSetup facetSetup = new FacetSetup { Id = "facets/ProductFacets", Facets = new List<Facet> { new Facet { Name = "Brand" } } };

			using (IDocumentSession docSession = store.OpenSession())
			{
				foreach (var productDoc in docSession.Query<Product>())
				{
					docSession.Delete(productDoc);
				}
				docSession.SaveChanges();

				docSession.Store(product1);
				docSession.Store(product2);
				docSession.Store(facetSetup);
				docSession.SaveChanges();
			}
		}

		public class Product
		{
			public Product(string name, string brand)
			{
				Name = name;
				Brand = brand;
			}

			public string Id { get; set; }
			public string Name { get; set; }
			public string Brand { get; set; }
		}

		public class Product_AvailableForSale2 : AbstractIndexCreationTask<Product>
		{
			public Product_AvailableForSale2()
			{
				Map = products => from p in products
								  select new
								  {
									  p.Name,
									  p.Brand,
									  Any = new object[]
												  {
													  p.Name,
													  p.Brand
												  }
									  };
			}
		}

		public class NoStaleQueriesListener : IDocumentQueryListener
		{
			public void BeforeQueryExecuted(IDocumentQueryCustomization queryCustomization)
			{
				queryCustomization.WaitForNonStaleResults(TimeSpan.FromSeconds(30));
			}
		}
	}
}