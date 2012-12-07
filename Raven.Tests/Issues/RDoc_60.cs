// -----------------------------------------------------------------------
//  <copyright file="RDoc-60.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System.Collections.Generic;
	using System.Linq;

	using Xunit;

	public class RDoc_60 : RavenTest
	{
		private class Order
		{
			public string Id { get; set; }

			public IList<LineItem> LineItems { get; set; }
		}

		private class LineItem
		{
			public string Id { get; set; }

			public string ProductId { get; set; }
		}

		private class Product
		{
			public string Id { get; set; }

			public string Name { get; set; }
		}

		[Fact]
		public void IncludeShouldWorkForStringIdentifiers()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var order = new Order
								{
									Id = "orders/1",
									LineItems = new List<LineItem>
									            {
										            new LineItem
										            {
											            Id = "lineItems/1",
														ProductId = "products/1"
										            }
									            }
								};

					var product = new Product
								  {
									  Id = "products/1",
									  Name = "Product 1"
								  };

					session.Store(product);
					session.Store(order);

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var order = session
						.Include<Order, Product>(x => x.LineItems.Select(li => li.ProductId))
						.Load("orders/1");

					foreach (var lineItem in order.LineItems)
					{
						var product = session.Load<Product>(lineItem.ProductId);
					}

					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}
			}
		}
	}
}