//-----------------------------------------------------------------------
// <copyright file="ShoppingCartItem.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Sample.EventSourcing
{
	public class ShoppingCartItem
	{
		public ShoppingCartItemProduct Product { get; set; }
		public int Quantity { get; set; }

		public override string ToString()
		{
			return string.Format("Product: {0}, Quantity: {1}", Product, Quantity);
		}
	}
}
