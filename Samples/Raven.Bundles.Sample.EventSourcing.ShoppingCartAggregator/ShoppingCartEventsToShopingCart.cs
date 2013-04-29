//-----------------------------------------------------------------------
// <copyright file="ShoppingCartEventsToShopingCart.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Linq;
using Raven.Database.Indexing;
using Raven.Json.Linq;

namespace Raven.Sample.EventSourcing
{
	[DisplayName("Aggregates/ShoppingCart")]
	public class ShoppingCartEventsToShopingCart : AbstractViewGenerator
	{
		public ShoppingCartEventsToShopingCart()
		{
			AddMapDefinition(docs => docs.Where(document => document.For == "ShoppingCart"));
			GroupByExtraction = source => source.ShoppingCartId;
			ReduceDefinition = Reduce;
			Indexes.Add("ShoppingCartId", FieldIndexing.NotAnalyzed);
			Indexes.Add("Aggregate", FieldIndexing.No);
			AddField("ShoppingCartId");
			AddField("Aggregate");
		}

		private static IEnumerable<object> Reduce(IEnumerable<dynamic> source)
		{
			foreach (var events in source
				.GroupBy(@event => @event.ShoppingCartId))
			{
				var cart = new ShoppingCart { Id = events.Key };
				foreach (var @event in events.OrderBy(x => x.Timestamp))
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
							cart.AddToCart(@event.ProductId, @event.ProductName, (decimal)@event.Price);
							break;
						case "Remove":
							cart.RemoveFromCart(@event.ProductId);
							break;
					}
				}
				yield return new
				{
					ShoppingCartId = cart.Id,
					Aggregate = RavenJObject.FromObject(cart)
				};
			}
		}
	}
}
