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
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class SlowIndex : RavenTest
	{
		[Fact]
		public void Test()
		{
			using (var documentStore = NewDocumentStore(requestedStorage: "munin"))
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

				using (var session = documentStore.OpenSession())
				{
					// query the index as of today to get the top customers over the last 30 days.
					var topCustomersDuringLast30Days = session.Query<OrderTotal, Orders_TotalByCustomerFor30Days>()
															  .OrderByDescending(x => x.Total)
															  .Where(x => x.Date == DateTime.Today)
															  .Take(10)
															  .ToList();
				}

				//WaitForUserToContinueTheTest(documentStore);
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
			// We have to use an object[] instead of an int[] or Enumerable.Range in order to get Raven to cooperate.
			// See http://issues.hibernatingrhinos.com/issue/RavenDB-757

			// ALSO - This is really slow for some reason.  It took over 36 seconds to index just 100 orders.
			// True, it is mapping 3000 entries, but that still shouldn't take that long.

			public Orders_TotalByCustomerFor30Days()
			{
				Map = orders => from order in orders
								from day in Enumerable.Range(1, 30)
								select new
								{
									order.CustomerId,
									Date = order.Placed.Date.AddDays(day - 1),
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