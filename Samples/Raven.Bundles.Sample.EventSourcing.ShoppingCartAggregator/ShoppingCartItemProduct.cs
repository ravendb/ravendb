//-----------------------------------------------------------------------
// <copyright file="ShoppingCartItemProduct.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Sample.EventSourcing
{
	public class ShoppingCartItemProduct
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public decimal Price { get; set; }

		public override string ToString()
		{
			return string.Format("Id: {0}, Name: {1}, Price: {2}", Id, Name, Price);
		}
	}
}
