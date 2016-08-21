using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class BadTransformer : RavenTestBase
    {
        [Fact]
        public async Task CanCreateTransformer()
        {
            using (var store = await GetDocumentStore())
            {
                new UserOrderSummaryTransformer().Execute(store);
            }
        }

        private class Order
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public IEnumerable<string> MerchantOrders { get; set; }
        }

        private class MerchantOrder
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string MerchantId { get; set; }
            public IEnumerable<MerchantOrderItem> Items { get; set; }
        }

        private class MerchantOrderItem
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Merchant
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class UserOrderSummaryTransformer : AbstractTransformerCreationTask<Order>
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

        private class UserOrderSummary
        {
            public int MerchantCount { get; set; }
            public IEnumerable<UserMerchantOrderSummary> MerchantOrders { get; set; }
            public string Id { get; set; }
            public int ItemCount { get; set; }
        }

        private class UserMerchantOrderSummary
        {
            public string MerchantId { get; set; }
            public string MerchantName { get; set; }
            public string MerchantOrderId { get; set; }
            public IEnumerable<MerchantOrderItem> Items { get; set; }
        }
    }

}
