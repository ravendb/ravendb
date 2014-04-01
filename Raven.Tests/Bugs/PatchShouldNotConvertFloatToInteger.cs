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
using Raven.Tests.Common;

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
						NestedDecimal = new NestedDecimalValue
						{
							DecimalValue = 0, 
							IntValue = 0, 
						},
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
			var json = store.DatabaseCommands.Get("products/1").DataAsJson;

			RavenJToken price;
			json.TryGetValue("Price", out price);
			Assert.Equal(JTokenType.Float, price.Type);

			RavenJToken quantity;
			json.TryGetValue("Quantity", out quantity);
			Assert.Equal(JTokenType.Integer, quantity.Type);

			// ---

			RavenJToken nestedDecimal;
			json.TryGetValue("NestedDecimal", out nestedDecimal);

			RavenJToken decimalValue;
			((RavenJObject) nestedDecimal).TryGetValue("DecimalValue", out decimalValue);
			Assert.Equal(JTokenType.Float, decimalValue.Type);

			RavenJToken intValue;
			((RavenJObject) nestedDecimal).TryGetValue("IntValue", out intValue);
			Assert.Equal(JTokenType.Integer, intValue.Type);

			// ---

			RavenJToken parentDecimalValue;
			json.TryGetValue("DecimalValue", out parentDecimalValue);
			Assert.Equal(JTokenType.Integer, parentDecimalValue.Type);

			RavenJToken parentIntValue;
			json.TryGetValue("IntValue", out parentIntValue);
			Assert.Equal(JTokenType.Float, parentIntValue.Type);
		}

		public class Product
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public decimal Price { get; set; }
			public int Quantity { get; set; }
			public NestedDecimalValue NestedDecimal { get; set; }

			// This is intended to be int, despite the name DecimalValue. We want to test that there is no conflict between the name of the nested object.
			public int DecimalValue { get; set; }
			public decimal IntValue { get; set; }
		}

		public class NestedDecimalValue
		{
			public decimal DecimalValue { get; set; }
			public int IntValue { get; set; }
		}
	}
}