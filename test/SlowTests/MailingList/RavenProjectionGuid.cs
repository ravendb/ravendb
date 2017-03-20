using System;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.MailingList
{
    public class RavenProjectionGuid : RavenTestBase
    {
        [Fact]
        public void TestProjectedGuid()
        {
            var accountId = Guid.NewGuid().ToString();

            using (var documentStore = GetDocumentStore())
            {
                new CustomerOrderProjection().Execute(documentStore);
                new CustomerOrderProjectionTeansformer().Execute(documentStore);

                //Load Test Data
                using (IDocumentSession session = documentStore.OpenSession())
                {
                    session.Store(new CustomerOrder() { Id = Guid.NewGuid().ToString(), AccountId = accountId, Status = "Pending", OrderDetails = "a left handed screwdriver" });
                    session.Store(new CustomerOrder() { Id = Guid.NewGuid().ToString(), AccountId = accountId, Status = "InProgress", OrderDetails = "a handfull of fairy dust" });
                    session.Store(new CustomerOrder() { Id = Guid.NewGuid().ToString(), AccountId = accountId, Status = "Delay", OrderDetails = "a long rest" });

                    session.SaveChanges();
                    session.Query<CustomerOrder>().Customize(x => x.WaitForNonStaleResults()).TransformWith<CustomerOrderProjectionTeansformer, AccountListItem>().Any();
                }

                using (IDocumentSession session = documentStore.OpenSession())
                {
                    var results =
                        session.Advanced.DocumentQuery<AccountListItem>("CustomerOrderProjection")
                            .WhereEquals("AccountId", accountId)
                            .WaitForNonStaleResults()
                            .SetTransformer("CustomerOrderProjectionTeansformer")
                            .ToList();
                    Assert.True(3 == results.Count);
                }
            }

        }

        private class CustomerOrder
        {
            public string Id { get; set; }
            public string AccountId { get; set; }
            public string Status { get; set; }
            public string OrderDetails { get; set; }
        }

        private class AccountListItem
        {
            public string Id { get; set; }
            public string AccountId { get; set; }
            public string Status { get; set; }
        }

        private class CustomerOrderProjection : AbstractIndexCreationTask<CustomerOrder>
        {
            public CustomerOrderProjection()
            {
                Map = orders => from o in orders
                                select new
                                {
                                    o.Id,
                                    o.AccountId,
                                    o.Status
                                };
            }
        }

        private class CustomerOrderProjectionTeansformer : AbstractTransformerCreationTask<CustomerOrder>
        {
            public CustomerOrderProjectionTeansformer()
            {
                TransformResults = orders =>
                                   from o in orders
                                   let item = LoadDocument<CustomerOrder>(o.Id.ToString())
                                   select new
                                   {
                                       Id = o.Id,
                                       AccountId = item.AccountId,
                                       Status = item.Status
                                   };
            }
        }
    }
}
