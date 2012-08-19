using System;
using System.Linq;
using Raven.Client.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ArrayOfMaybeNull : RavenTest
	{
		public class Orders_Search : AbstractIndexCreationTask<Order>
		{
			public class ReduceResult
			{
				public string Query { get; set; }
				public DateTime OrderedAt { get; set; }
			}

			public Orders_Search()
			{
				Map = orders => from order in orders
				                select new
				                {
				                	Query = new[]
				                	{
				                		order.FirstName,
				                		order.LastName,
				                		order.OrderNumber,
				                		order.Email,
				                		order.CompanyName
				                	},
				                	order.OrderedAt
				                };
			}
		}

		public class Order
		{
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public string OrderNumber { get; set; }
			public string Email { get; set; }
			public string CompanyName { get; set; }
			public DateTime OrderedAt { get; set; }
		}

		[Fact]
		public void CanPerformSearch()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Order
					{
						CompanyName = null,
						Email = "ayende@ayende.com",
						FirstName = "Oren",
						LastName = "Eini",
						OrderNumber = "E12312",
						OrderedAt = DateTime.Now
					});
					session.SaveChanges();
				}

				new Orders_Search().Execute(store);

				using (var session = store.OpenSession())
				{
					var orders = session.Query<Orders_Search.ReduceResult, Orders_Search>()
						.Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
						.Where(x => x.Query == "oren")
						.As<Order>()
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.NotEmpty(orders);
				}
			}
		}
	}
}