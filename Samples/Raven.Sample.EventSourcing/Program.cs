//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Document;
using System.Linq;
using Raven.Json.Linq;

namespace Raven.Sample.EventSourcing
{
	class Program
	{
		static void Main()
		{
			var documentStore1 = new DocumentStore { Url = "http://localhost:8080" }.Initialize();

			var events = new object[]
				{
					new
					{
						For = "ShoppingCart",
						Type = "Create",
						Timestamp = DateTime.Now,
						ShoppingCartId = "shoppingcarts/12",
						CustomerId = "users/ayende",
						CustomerName = "Ayende Rahien"
					},
					new
					{
						For = "ShoppingCart",
						Type = "Add",
						Timestamp = DateTime.Now,
						ShoppingCartId = "shoppingcarts/12",
						ProductId = "products/8123",
						ProductName = "Fish & Chips",
						Price = 8.5m
					},
					new
					{
						For = "ShoppingCart",
						Type = "Add",
						Timestamp = DateTime.Now,
						ShoppingCartId = "shoppingcarts/12",
						ProductId = "products/3214",
						ProductName = "Guinness",
						Price = 2.1m
					},
					new
					{
						For = "ShoppingCart",
						Type = "Remove",
						Timestamp = DateTime.Now,
						ShoppingCartId = "shoppingcarts/12",
						ProductId = "products/8123"
					},
					new
					{
						For = "ShoppingCart",
						Type = "Add",
						Timestamp = DateTime.Now,
						ShoppingCartId = "shoppingcarts/12",
						ProductId = "products/8121",
						ProductName = "Beef Pie",
						Price = 9.0m
					},
				};

			int i = 1;
			foreach (var @event in events)
			{
				documentStore1.DatabaseCommands.Put("events/" + i++, null, RavenJObject.FromObject(@event), new RavenJObject());                
			}

			Console.WriteLine("Wrote {0} events", events.Length);

			Console.ReadLine();

			using(var session = documentStore1.OpenSession())
			{
				var aggregate = session.Advanced.LuceneQuery<AggregateHolder>("Aggregates/ShoppingCart")
					.Where("ShoppingCartId:shoppingcarts/12")
					.Single();

				var cart = new JsonSerializer().Deserialize<ShoppingCart>(new RavenJTokenReader(aggregate.Aggregate));

				Console.WriteLine(cart.Total);
			}
		}
	}

	public class AggregateHolder
	{
		public string ShoppingCartId { get; set; }
		public RavenJObject Aggregate { get; set; }
	}
}
