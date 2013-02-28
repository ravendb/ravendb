//-----------------------------------------------------------------------
// <copyright file="ShoppingCart.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;

namespace Raven.Sample.EventSourcing
{
	public class ShoppingCart
	{
		public string Id { get; set; }
		public ShoppingCartCustomer Customer { get; set; }
		public List<ShoppingCartItem> Items { get; set; }
		public decimal Total { get { return Items.Sum(x => x.Product.Price * x.Quantity); } }

		public ShoppingCart()
		{
			Items = new List<ShoppingCartItem>();
		}

		public void AddToCart(string productId, string productName, decimal price)
		{
			var item = Items.FirstOrDefault(x => x.Product.Id == productId);
			if (item != null)
			{
				item.Quantity++;
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
				Quantity = 1
			});
		}

		public void RemoveFromCart(string productId)
		{
			var shoppingCartItem = Items.FirstOrDefault(x => x.Product.Id == productId);
			if (shoppingCartItem == null)
				return;
			Items.Remove(shoppingCartItem);
		}

		public override string ToString()
		{
			return string.Format("Id: {0}, Customer: {1}, Items: {2}", Id, Customer, string.Join(", ", Items));
		}
	}
}
