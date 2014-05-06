using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class Orders : RavenTest
	{
		[Fact]
		public void CanCreateIndex()
		{
			using(var store = NewDocumentStore())
			{
				new Orders_Search().Execute(store);
			}
		}

		public class Order
		{
			public string Id { get; set; }

			public string OrderNumber { get; set; }

			public List<Payment> Payments { get; set; }

			public Order()
			{
				Payments = new List<Payment>();
			}
		}

		public class Payment
		{
			public string PaymentIdentifier { get; set; }
			public Money Total { get; set; }
			public Money VAT { get; set; }
			public DateTime At { get; set; }
			public string Link { get; set; }
		}

		public class Money
		{
			public string Currency { get; set; }
			public decimal Amount { get; set; }
		}

		public class Orders_Search : AbstractIndexCreationTask<Order, Orders_Search.ReduceResult>
		{
			public class ReduceResult
			{
				public string Query { get; set; }
				public DateTime LastPaymentDate { get; set; }
			}

			public Orders_Search()
			{
				Map = orders => from order in orders
				                //let lastPayment = order.Payments[order.Payments.Count-1]
				                select new
				                {
				                	Query = new object[]
				                	{
				                		order.OrderNumber,
				                		order.Payments.Select(x => x.PaymentIdentifier)
				                	},
				                	LastPaymentDate = order.Payments.Last().At
				                };
			}
		}
	}
}