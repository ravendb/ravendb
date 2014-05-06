// -----------------------------------------------------------------------
//  <copyright file="SlowIndex.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class SlowIndex : RavenTest
	{
		[Fact]
		public void Test()
		{
			using (var documentStore = NewDocumentStore(requestedStorage: "voron"))
			{
				documentStore.Initialize();
				var ordersTotalByCustomerFor30Days = new Orders_TotalByCustomerFor30Days();
				documentStore.ExecuteIndex(ordersTotalByCustomerFor30Days);

				var stopwatch = new Stopwatch();
				stopwatch.Start();

				// generate random orders over the past few months
				const int numOrdersToGenerate = 100;
				GenerateRandomOrders(documentStore, numOrdersToGenerate, DateTime.Today.AddMonths(-3), DateTime.Today);
				Debug.WriteLine("Generated {0} orders in {1} ms", numOrdersToGenerate, stopwatch.ElapsedMilliseconds);

				stopwatch.Restart();

				// Manually wait for the index
				while (documentStore.DatabaseCommands.GetStatistics().StaleIndexes.Contains(ordersTotalByCustomerFor30Days.IndexName))
					Thread.Sleep(100);

				stopwatch.Stop();
				Debug.WriteLine("Took {0} ms to finish indexing", stopwatch.ElapsedMilliseconds);

				// It should be less, but at 20 seconds, there's definitely something wrong
				Assert.InRange(stopwatch.ElapsedMilliseconds, 0, 20000);

				//using (var session = documentStore.OpenSession())
				//{
				//	// example of how to query the index as of today to get the top customers over the last 30 days.
				//	var topCustomersDuringLast30Days = session.Query<OrderTotal, Orders_TotalByCustomerFor30Days>()
				//											  .OrderByDescending(x => x.Total)
				//											  .Where(x => x.Date == DateTime.Today)
				//											  .Take(10)
				//											  .ToList();
				//}
			}
		}

		public static void GenerateRandomOrders(IDocumentStore documentStore, int numOrders, DateTime starting, DateTime ending)
		{
			using (var session = documentStore.OpenSession())
			{
				var names = new[] {
                                      "Alice", "Bob", "Charlie", "David", "Ethel", "Frank", "George",
                                      "Henry", "Iris", "Josh", "Kelly", "Larry", "Mike", "Natalie",
                                      "Oscar", "Paul", "Quincy", "Rose", "Sam", "Tina", "Uma", "Victor",
                                      "William", "Xavier", "Yvonne", "Zach"
                                  };

				var random = new Random();

				var customers = names.Select(name => new Customer { Name = name }).ToList();

				foreach (var customer in customers)
					session.Store(customer);

				for (int i = 0; i < numOrders; i++)
				{
					var customer = customers[random.Next(customers.Count)];
					var amount = random.Next(100000) / 100m;

					var range = ending - starting;
					var placed = starting.AddSeconds(random.Next((int)range.TotalSeconds));

					session.Store(new Order { CustomerId = customer.Id, Amount = amount, Placed = placed });
				}

				session.SaveChanges();
			}
		}

		public class Customer
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Order
		{
			public string Id { get; set; }
			public string CustomerId { get; set; }
			public decimal Amount { get; set; }
			public DateTime Placed { get; set; }
		}

		public class OrderTotal
		{
			public string CustomerId { get; set; }
			public DateTime Date { get; set; }
			public decimal Total { get; set; }
		}

		public class Orders_TotalByCustomerFor30Days : AbstractIndexCreationTask<Order, OrderTotal>
		{
			// For the 30 days following the order, the order will be included in that day's totals
			
			public Orders_TotalByCustomerFor30Days()
			{
				Map = orders => from order in orders
								from day in Enumerable.Range(0, 30)
								select new
								{
									order.CustomerId,
									Date = order.Placed.Date.AddDays(day),
									Total = order.Amount,
								};

				Reduce = results => from result in results
									group result by new { result.CustomerId, result.Date }
										into g
										select new
										{
											g.Key.CustomerId,
											g.Key.Date,
											Total = g.Sum(x => x.Total),
										};

				Sort(x => x.Total, SortOptions.Double);
			}
		} 
	}
}