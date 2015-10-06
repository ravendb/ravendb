using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class BadTransformer : RavenTestBase
	{
		[Fact]
		public void CanCreateTransformer()
		{
			using (var store = NewDocumentStore())
			{
				new UserOrderSummaryTransformer().Execute(store);
			}
		}

		public class Order
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public IEnumerable<string> MerchantOrders { get; set; }
		}

		public class MerchantOrder
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string MerchantId { get; set; }
			public IEnumerable<MerchantOrderItem> Items { get; set; }
		}

		public class MerchantOrderItem
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Merchant
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class UserOrderSummaryTransformer : AbstractTransformerCreationTask<Order>
		{
			public UserOrderSummaryTransformer()
			{
				TransformResults = orders => from order in orders
											 let merchantOrders = order.MerchantOrders.Select(LoadDocument<MerchantOrder>)
											 select new UserOrderSummary
											 {
												 Id = order.Id,
												 MerchantOrders = from mo in merchantOrders
																  let merchant = LoadDocument<Merchant>(mo.MerchantId)
																  select new UserMerchantOrderSummary
																  {
																	  MerchantId = merchant.Id,
																	  MerchantName = merchant.Name,
																	  MerchantOrderId = mo.Id,
																	  Items = from i in mo.Items
																			  select new MerchantOrderItem
																			  {
																				  Id = i.Id,
																				  Name = i.Name
																			  }
																  }
											 };
			}
		}

		public class UserOrderSummary
		{
			public int MerchantCount { get; set; }
			public IEnumerable<UserMerchantOrderSummary> MerchantOrders { get; set; }
			public string Id { get; set; }
			public int ItemCount { get; set; }
		}

		public class UserMerchantOrderSummary
		{
			public string MerchantId { get; set; }
			public string MerchantName { get; set; }
			public string MerchantOrderId { get; set; }
			public IEnumerable<MerchantOrderItem> Items { get; set; }
		}
	}

}