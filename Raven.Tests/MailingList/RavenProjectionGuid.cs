using Xunit;
using System.Linq;
using Raven.Client.Indexes;
using System;
using System.Reflection;
using Raven.Client;

namespace Raven.Tests.MailingList
{
	public class RavenProjectionGuid : RavenTest
	{
		[Fact]
		public void TestProjectedGuid()
		{
			Guid accountId = Guid.NewGuid();

			using (var documentStore = NewDocumentStore())
			{
				new CustomerOrderProjection().Execute(documentStore);

				//Load Test Data
				using (IDocumentSession session = documentStore.OpenSession())
				{
					session.Store(new CustomerOrder() { Id = Guid.NewGuid(), AccountId = accountId, Status = "Pending", OrderDetails = "a left handed screwdriver" });
					session.Store(new CustomerOrder() { Id = Guid.NewGuid(), AccountId = accountId, Status = "InProgress", OrderDetails = "a handfull of fairy dust" });
					session.Store(new CustomerOrder() { Id = Guid.NewGuid(), AccountId = accountId, Status = "Delay", OrderDetails = "a long rest" });

					session.SaveChanges();
					session.Query<CustomerOrder>().Customize(x => x.WaitForNonStaleResults()).Any();
				}

				using (IDocumentSession session = documentStore.OpenSession())
				{
					var results = 
						session.Advanced.LuceneQuery<AccountListItem>("CustomerOrderProjection")
							.WhereEquals("AccountId", accountId)
							.WaitForNonStaleResults()
							.ToList();
					Assert.True(3 == results.Count);
				}
			}

		}


		public class CustomerOrder
		{
			public Guid Id { get; set; }
			public Guid AccountId { get; set; }
			public string Status { get; set; }
			public string OrderDetails { get; set; }
		}

		public class AccountListItem
		{
			public string Id { get; set; }
			public Guid AccountId { get; set; }
			public string Status { get; set; }
		}

		public class CustomerOrderProjection : AbstractIndexCreationTask<CustomerOrder, AccountListItem>
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

				TransformResults = (database, orders) =>
				                   from o in orders
				                   let item = database.Load<CustomerOrder>(o.Id.ToString())
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