using System;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class MultiOutputReduce : RavenTest
	{
		[Fact]
		public void CanGetCorrectResultsFromAllItems()
		{
			for (int xx = 0; xx < 50; xx++)
			{
				using (var store = NewDocumentStore(requestedStorage: "munin"))
				{
					new Orders_Search().Execute(store);

					for (int i = 0; i < 12; i++)
					{
						using (var session = store.OpenSession())
						{
							var customerId = "customers/" + i;
							session.Store(new Customer
							{
								Id = customerId,
								Name = "oren"
							});

							for (int j = 0; j < 5; j++)
							{
								session.Store(new Order
								{
									CustomerId = customerId,
									Id = "orders/" + i + "/" + j
								});
							}

							session.SaveChanges();
						}
					}

					using (var session = store.OpenSession())
					{
						var searchResults = session.Query<SearchResult, Orders_Search>()
						                           .Customize(x => x.WaitForNonStaleResults())
						                           .Where(x => x.OrderId != null)
						                           .ToList();
						Assert.Equal(12*5, searchResults.Count);
						foreach (var searchResult in searchResults)
						{
							Assert.Equal("oren", searchResult.CustomerName);
						}
					}
				}
			}
		}

		private class Customer
		{
			public string Id;
			public string Name;
		}

		private class Order
		{
			public string Id;
			public string CustomerId;
		}

		public class SearchResult
		{
			public string CustomerName;
			public string OrderId;
			public string CustomerId;
		}

		public class Orders_Search : AbstractMultiMapIndexCreationTask<SearchResult>
		{
			public Orders_Search()
			{
				AddMap<Customer>(customers =>
				                 from customer in customers
				                 select new
				                 {
					                 CustomerId = customer.Id,
					                 CustomerName = customer.Name,
					                 OrderId = (string) null
				                 });

				AddMap<Order>(orders =>
				              from order in orders
				              select new
				              {
					              OrderId = order.Id,
					              CustomerName = (string) null,
					              order.CustomerId
				              });

				Reduce = results =>
				         from searchResult in results
				         group searchResult by searchResult.CustomerId
				         into g
				         let customerName = g.FirstOrDefault(x => x.CustomerName != null).CustomerName
				         from item in g
				         select new
				         {
					         CustomerName = customerName,
					         item.OrderId,
					         item.CustomerId
				         };
			}
		}
	}
}