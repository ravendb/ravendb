//-----------------------------------------------------------------------
// <copyright file="ShoppingCartEventsToShopingCart.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Linq;

namespace Raven.Tests.Indexes
{
	[CLSCompliant(false)]
	[DisplayName("Aggregates/ShoppingCart")]
	public class ShoppingCartEventsToShopingCart : AbstractViewGenerator
	{
		public ShoppingCartEventsToShopingCart()
		{
			AddMapDefinition(docs => EventsToShoppingCart(docs.Where(document => document.For == "ShoppingCart")));
			GroupByExtraction = source => source.ShoppingCartId;
			ReduceDefinition = Reduce;

			AddField("ShoppingCartId");
			AddField("Aggregate");

			Indexes.Add("ShoppingCartId", FieldIndexing.NotAnalyzed);
			Indexes.Add("Aggregate", FieldIndexing.No);
		}

		private static IEnumerable<object> Reduce(IEnumerable<dynamic> source)
		{
			foreach (var carts in source
				.GroupBy(cart => cart.ShoppingCartId))
			{
				var cart = new ShoppingCart {Id = carts.Key};

				var enumerable = carts.Select(x => x.Aggregate).ToArray();
				cart.Merge(enumerable);

				yield return new
				{
					ShoppingCartId = cart.Id,
					Aggregate = cart
				};
			}
		}

		private static IEnumerable<object> EventsToShoppingCart(IEnumerable<dynamic> source)
		{
			foreach (var @event in source)
			{
				var cart = new ShoppingCart { Id = @event.ShoppingCartId };
				{
					switch ((string)@event.Type)
					{
						case "Create":
							cart.Customer = new ShoppingCartCustomer
							{
								Id = @event.CustomerId,
								Name = @event.CustomerName
							};
							break;
						case "Add":
							cart.AddToCart(@event.ProductId, @event.ProductName, (decimal)@event.Price, 1);
							break;
						case "Remove":
							cart.AddToCart(@event.ProductId, @event.ProductName, (decimal)@event.Price, -1);
							break;
					}
				}
				yield return new
				{
					@event.__document_id,
					ShoppingCartId = cart.Id,
					Aggregate = cart
				};
			}
		}

		public class ShoppingCart
		{
			public string Id { get; set; }
			public ShoppingCartCustomer Customer { get; set; }
			public List<ShoppingCartItem> Items { get; set; }
			public decimal Total { get { return Items.Where(x=>x.Quantity > 0).Sum(x => x.Product.Price * x.Quantity); } }

			public int ItemsCount
			{
				get { return Items.Count(x => x.Quantity > 0);  }
			}

			public ShoppingCart()
			{
				Items = new List<ShoppingCartItem>();
			}

			public void AddToCart(string productId, string productName, decimal price, int quantity)
			{
				var item = Items.FirstOrDefault(x => x.Product.Id == productId);
				if (item != null)
				{
					item.Quantity+=quantity;
					return;
				}
				Items.Add(new ShoppingCartItem
				{
					Product = new ShoppingCartItemProduct
					{
						Id = productId,
						Name = productName,
						Price = price
					},
					Quantity = quantity
				});
			}

			public void Merge(IEnumerable<dynamic> carts)
			{
				foreach (var cart in carts)
				{
					if(cart.Customer != null)
					{
						Customer = new ShoppingCartCustomer
						{
							Id = cart.Customer.Id,
							Name = cart.Customer.Name
						};
					}
					if(cart.Items != null)
					{
						foreach (var item in cart.Items)
						{
							decimal price = (decimal)item.Product.Price;
							int quantity = item.Quantity;
							AddToCart(item.Product.Id, item.Product.Name, price, quantity);
						}
					}
				}
			}
		}
		public class ShoppingCartCustomer
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class ShoppingCartItem
		{
			public ShoppingCartItemProduct Product { get; set; }
			public int Quantity { get; set; }

		}

		public class ShoppingCartItemProduct
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public decimal Price { get; set; }
		}
	}
}