using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Everett616 : RavenTest
	{
		[Fact]
		public void CanIndexWithNoErrors_DatetimeOffset()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("test/1", null,
				                           RavenJObject.Parse(
				                           	@"{
  '$type': 'Domain.Model.Clicks.ClickAllocation, Domain',
  'AccountId': 'accounts/4',
  'Quantity': 90,
  'Date': '2011-12-12T08:47:44.0706445-05:00',
  'Key': null,
  'OrderNumber': null,
  'PurchaseOrderNumber': null,
  'PurchaseDate': '0001-01-01T00:00:00.0000000-05:00',
  'ReorderQuantity': 0,
  'Type': 'Dealer',
  'LastSavedDate': '2011-12-12T08:47:44.1643945-05:00',
  'LastSavedUser': 'NLWEB$/NETLABELS (NLWEB)',
  'SourceId': '00000000-0000-0000-0000-000000000000'
}"),
				                           new RavenJObject
				                           {
				                           	{Constants.RavenEntityName, "ClickAllocations"}
				                           });

				store.DatabaseCommands.Put("test/2", null,
									   RavenJObject.Parse(
										@"{
  '$type': 'Domain.Model.Clicks.ClickAllocation, Domain',
  'AccountId': 'accounts/4',
  'Quantity': 20,
  'Date': '2012-02-28T16:05:18.7359910Z',
  'Key': null,
  'OrderNumber': null,
  'PurchaseOrderNumber': null,
  'PurchaseDate': '0001-01-01T00:00:00.0000000',
  'ReorderQuantity': 5,
  'Type': 'Dealer',
  'LastSavedDate': '2012-02-28T16:05:19.3609910',
  'LastSavedUser': 'NLWEB$/NETLABELS (NLWEB)',
  'SourceId': '00000000-0000-0000-0000-000000000000'
}"),
									   new RavenJObject
				                           {
				                           	{Constants.RavenEntityName, "ClickAllocations"}
				                           });


				store.DatabaseCommands.PutIndex("test",
				                                new IndexDefinition
				                                {
				                                	Map =
				                                	@"docs.ClickAllocations
	.Select(doc => new {AccountId = doc.AccountId, Date = doc.Date, Id = doc.__document_id, Key = doc.Key, LastSavedDate = doc.LastSavedDate, LastSavedUser = doc.LastSavedUser, OrderNumber = doc.OrderNumber, PurchaseDate = doc.PurchaseDate, PurchaseOrderNumber = doc.PurchaseOrderNumber, Quantity = doc.Quantity, ReorderQuantity = doc.ReorderQuantity, Type = doc.Type})
",
				                                	Reduce =
				                                	@"results
	.GroupBy(result => result.AccountId)
	.Select(a => new {a = a, clickAllocation = a.OrderByDescending(x => x.Date).FirstOrDefault()})
	.Select(__h__TransparentIdentifier0 => new {AccountId = __h__TransparentIdentifier0.clickAllocation.AccountId, Date = __h__TransparentIdentifier0.clickAllocation.Date, Id = __h__TransparentIdentifier0.clickAllocation.Id, Key = __h__TransparentIdentifier0.clickAllocation.Key, LastSavedDate = __h__TransparentIdentifier0.clickAllocation.LastSavedDate, LastSavedUser = __h__TransparentIdentifier0.clickAllocation.LastSavedUser, OrderNumber = __h__TransparentIdentifier0.clickAllocation.OrderNumber, PurchaseDate = __h__TransparentIdentifier0.clickAllocation.PurchaseDate, PurchaseOrderNumber = __h__TransparentIdentifier0.clickAllocation.PurchaseOrderNumber, Quantity = __h__TransparentIdentifier0.clickAllocation.Quantity, ReorderQuantity = __h__TransparentIdentifier0.clickAllocation.ReorderQuantity, Type = __h__TransparentIdentifier0.clickAllocation.Type})"
				                                });

				WaitForIndexing(store);
				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}

		[Fact]
		public void CanIndexWithNoErrors_Datetime()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("test/1", null,
										   RavenJObject.Parse(
											@"{
  '$type': 'Domain.Model.Clicks.ClickAllocation, Domain',
  'AccountId': 'accounts/4',
  'Quantity': 90,
  'Date': '2011-12-12T08:47:44.0706445',
  'Key': null,
  'OrderNumber': null,
  'PurchaseOrderNumber': null,
  'PurchaseDate': '0001-01-01T00:00:00.0000000-05:00',
  'ReorderQuantity': 0,
  'Type': 'Dealer',
  'LastSavedDate': '2011-12-12T08:47:44.1643945-05:00',
  'LastSavedUser': 'NLWEB$/NETLABELS (NLWEB)',
  'SourceId': '00000000-0000-0000-0000-000000000000'
}"),
										   new RavenJObject
				                           {
				                           	{Constants.RavenEntityName, "ClickAllocations"}
				                           });

				store.DatabaseCommands.Put("test/2", null,
									   RavenJObject.Parse(
										@"{
  '$type': 'Domain.Model.Clicks.ClickAllocation, Domain',
  'AccountId': 'accounts/4',
  'Quantity': 20,
  'Date': '2012-02-28T16:05:18.7359910',
  'Key': null,
  'OrderNumber': null,
  'PurchaseOrderNumber': null,
  'PurchaseDate': '0001-01-01T00:00:00.0000000',
  'ReorderQuantity': 5,
  'Type': 'Dealer',
  'LastSavedDate': '2012-02-28T16:05:19.3609910',
  'LastSavedUser': 'NLWEB$/NETLABELS (NLWEB)',
  'SourceId': '00000000-0000-0000-0000-000000000000'
}"),
									   new RavenJObject
				                           {
				                           	{Constants.RavenEntityName, "ClickAllocations"}
				                           });


				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
												{
													Map =
													@"docs.ClickAllocations
	.Select(doc => new {AccountId = doc.AccountId, Date = doc.Date, Id = doc.__document_id, Key = doc.Key, LastSavedDate = doc.LastSavedDate, LastSavedUser = doc.LastSavedUser, OrderNumber = doc.OrderNumber, PurchaseDate = doc.PurchaseDate, PurchaseOrderNumber = doc.PurchaseOrderNumber, Quantity = doc.Quantity, ReorderQuantity = doc.ReorderQuantity, Type = doc.Type})
",
													Reduce =
													@"results
	.GroupBy(result => result.AccountId)
	.Select(a => new {a = a, clickAllocation = a.OrderByDescending(x => x.Date).FirstOrDefault()})
	.Select(__h__TransparentIdentifier0 => new {AccountId = __h__TransparentIdentifier0.clickAllocation.AccountId, Date = __h__TransparentIdentifier0.clickAllocation.Date, Id = __h__TransparentIdentifier0.clickAllocation.Id, Key = __h__TransparentIdentifier0.clickAllocation.Key, LastSavedDate = __h__TransparentIdentifier0.clickAllocation.LastSavedDate, LastSavedUser = __h__TransparentIdentifier0.clickAllocation.LastSavedUser, OrderNumber = __h__TransparentIdentifier0.clickAllocation.OrderNumber, PurchaseDate = __h__TransparentIdentifier0.clickAllocation.PurchaseDate, PurchaseOrderNumber = __h__TransparentIdentifier0.clickAllocation.PurchaseOrderNumber, Quantity = __h__TransparentIdentifier0.clickAllocation.Quantity, ReorderQuantity = __h__TransparentIdentifier0.clickAllocation.ReorderQuantity, Type = __h__TransparentIdentifier0.clickAllocation.Type})"
												});

				WaitForIndexing(store);

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}
		
		protected override void CreateDefaultIndexes(Client.IDocumentStore documentStore)
		{
		}
	}
}