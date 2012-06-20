// -----------------------------------------------------------------------
//  <copyright file="Brett.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Reflection;
using System.Web.UI.WebControls;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Brett : RavenTest
	{
		[Fact]
		public void TestMultiMap()
		{
			Guid accountId = Guid.NewGuid();

			using (var store = NewDocumentStore())
			{

				//Load Test Data
				using (IDocumentSession session = store.OpenSession())
				{
					session.Store(new OrderHardware()
					{Id = Guid.NewGuid().ToString(), AccountId = accountId, RequestStatus = "Pending", CustomerDetails = "Si & Co"});
					session.Store(new OrderHardware()
					{
						Id = Guid.NewGuid().ToString(),
						AccountId = accountId,
						RequestStatus = "InProgress",
						CustomerDetails = "Baileyish"
					});
					session.Store(new OrderHardware()
					{Id = Guid.NewGuid().ToString(), AccountId = accountId, RequestStatus = "Delay", CustomerDetails = "Babu Ltd"});
					session.Store(new OrderSubscription()
					{
						Id = Guid.NewGuid().ToString(),
						AccountId = accountId,
						RequestStatus = "Submitted",
						NewSimNumber = "1122344324343"
					});
					session.Store(new OrderSubscription()
					{
						Id = Guid.NewGuid().ToString(),
						AccountId = accountId,
						RequestStatus = "Actioning",
						NewSimNumber = "1122344324343"
					});
					session.Store(new OrderSiteInstall()
					{Id = Guid.NewGuid().ToString(), AccountId = accountId, RequestStatus = "Cancelled", OrderDetails = "10 Handsets"});
					session.Store(new OrderSiteInstall()
					{Id = Guid.NewGuid().ToString(), AccountId = accountId, RequestStatus = "InProgress", OrderDetails = "20 Handsets"});
					session.Store(new OrderSiteInstall()
					{Id = Guid.NewGuid().ToString(), AccountId = accountId, RequestStatus = "Submitted", OrderDetails = "Data Only"});
					session.Store(new OrderSiteInstall()
					{Id = Guid.NewGuid().ToString(), AccountId = accountId, RequestStatus = "Fulfilling", OrderDetails = "20 iPhone"});

					session.SaveChanges();
					session.Query<OrderSiteInstall>().Customize(x => x.WaitForNonStaleResults()).Any();
					session.Query<OrderSubscription>().Customize(x => x.WaitForNonStaleResults()).Any();
					session.Query<OrderHardware>().Customize(x => x.WaitForNonStaleResults()).Any();
				}

				new ListItemIndex().Execute(store);

				using (IDocumentSession session = store.OpenSession())
				{
					var query = session.Advanced.LuceneQuery<IListItem>(typeof (ListItemIndex).Name)
						.WaitForNonStaleResultsAsOfNow();
					query.WhereEquals("AccountId", accountId);
					var results = query.ToList();

					Assert.Equal(9, results.Count);
				}
			}
		}


		protected void DeleteAll<T>(IDocumentStore documentStore)
		{
			using (IDocumentSession session = documentStore.OpenSession())
			{
				foreach (var doc in session.Query<T>().ToList())
				{
					session.Delete(doc);
				}

				session.SaveChanges();
				session.Query<T>().Customize(x => x.WaitForNonStaleResults()).Any();
			}
		}

		public class OrderHardware : IListItem
		{
			public string Id { get; set; }
			public Guid AccountId { get; set; }
			public string RequestStatus { get; set; }
			public string CustomerDetails { get; set; }
		}

		public class OrderSubscription : IListItem
		{
			public string Id { get; set; }
			public Guid AccountId { get; set; }
			public string RequestStatus { get; set; }
			public string PhoneNumber { get; set; }
			public string NewSimNumber { get; set; }
		}

		public class OrderSiteInstall : IListItem
		{
			public string Id { get; set; }
			public Guid AccountId { get; set; }
			public string RequestStatus { get; set; }
			public string OrderDetails { get; set; }
		}

		// This interface is the compromise 
		public interface IListItem
		{
			string Id { get; set; }
			Guid AccountId { get; set; }
			string RequestStatus { get; set; }
		}

		// This interface is what I want to get
		public class GridListItem
		{
			string Id { get; set; }
			Guid AccountId { get; set; }
			string RequestStatus { get; set; }
		}

		public class ListItemIndex : AbstractMultiMapIndexCreationTask<ListItemIndex.Result>
		{
			public class Result
			{
				public string Id { get; set; }
				public Guid AccountId { get; set; }
				public string RequestStatus { get; set; }
			}
			public ListItemIndex()
			{
				AddMap<OrderHardware>(order => from o in order
											   select new
											   {
												   o.Id,
												   o.AccountId,
												   o.RequestStatus,
											   });
				AddMap<OrderSubscription>(order => from o in order
												   select new
												   {
													   o.Id,
													   o.AccountId,
													   o.RequestStatus,
												   });
				AddMap<OrderSiteInstall>(order => from o in order
												  select new
												  {
													  o.Id,
													  o.AccountId,
													  o.RequestStatus,
												  });
			}
		}
	}
}