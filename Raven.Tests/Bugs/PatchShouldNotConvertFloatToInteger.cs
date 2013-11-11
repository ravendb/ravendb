//-----------------------------------------------------------------------
// <copyright file="AnalyzerPerField.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class PatchShouldNotConvertFloatToInteger : RavenTest
	{
		[Fact]
		public void ShouldKeepIntegerAsIntegerAndFloatAsFloat()
		{
			using (var store = NewDocumentStore(requestedStorage:"esent"))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Product
					{
						Name = "Product 1",
						Price = 0,
						Quantity = 0,
					});
					session.SaveChanges();
				}
				AssertThatPriceIsDecimal(store);

				store.DatabaseCommands.Patch("products/1", new ScriptedPatchRequest
				{
					Script = "this.Name = 'foo';",
				});
				AssertThatPriceIsDecimal(store);
				
				using (var session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;

					// If the Price value changed from 0.0 to 0, 
					// the following line will resulted in another PUT request to change it back to 0.0. 
					var product1 = session.Query<Product>().FirstOrDefault();
					
					session.Store(new Product
					{
						Name = "Product 2",
					});
					session.SaveChanges();
				}
				AssertThatPriceIsDecimal(store);
			}
		}

		private void AssertThatPriceIsDecimal(IDocumentStore store)
		{
			RavenJToken price;
			store.DatabaseCommands.Get("products/1").DataAsJson.TryGetValue("Price", out price);
			Assert.Equal(JTokenType.Float, price.Type);

			RavenJToken quantity;
			store.DatabaseCommands.Get("products/1").DataAsJson.TryGetValue("Quantity", out quantity);
			Assert.Equal(JTokenType.Integer, quantity.Type);
		}

		public class Product
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public decimal Price { get; set; }
			public int Quantity { get; set; }
		}
	}
}