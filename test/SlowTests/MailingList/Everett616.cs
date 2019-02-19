using System.Collections.Generic;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Everett616 : RavenTestBase
    {
        [Fact]
        public void CanIndexWithNoErrors_DatetimeOffset()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var json = commands.ParseJson(LinuxTestUtils.Dos2Unix(@"{
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
}"));

                    commands.Put("test/1", null, json, new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "ClickAllocations" } });

                    json = commands.ParseJson(LinuxTestUtils.Dos2Unix(@"{
  '$type': 'Domain.Model.Clicks.ClickAllocation, Domain',
  'AccountId': 'accounts/4',
  'Quantity': 20,
  'Date': '2012-02-28T16:05:18.7359910+00:00',
  'Key': null,
  'OrderNumber': null,
  'PurchaseOrderNumber': null,
  'PurchaseDate': '0001-01-01T00:00:00.0000000',
  'ReorderQuantity': 5,
  'Type': 'Dealer',
  'LastSavedDate': '2012-02-28T16:05:19.3609910',
  'LastSavedUser': 'NLWEB$/NETLABELS (NLWEB)',
  'SourceId': '00000000-0000-0000-0000-000000000000'
}"));

                    commands.Put("test/2", null, json, new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "ClickAllocations" } });
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {
                                                new IndexDefinition
                                                {
                                                    Name = "test",
                                                    Maps = {
                                                        LinuxTestUtils.Dos2Unix(@"docs.ClickAllocations
    .Select(doc => new {AccountId = doc.AccountId, Date = doc.Date, Id = Id(doc), Key = doc.Key, LastSavedDate = doc.LastSavedDate, LastSavedUser = doc.LastSavedUser, OrderNumber = doc.OrderNumber, PurchaseDate = doc.PurchaseDate, PurchaseOrderNumber = doc.PurchaseOrderNumber, Quantity = doc.Quantity, ReorderQuantity = doc.ReorderQuantity, Type = doc.Type})
") },
                                                    Reduce =
                                                        LinuxTestUtils.Dos2Unix(@"results
    .GroupBy(result => result.AccountId)
    .Select(a => new {a = a, clickAllocation = a.OrderByDescending(x => x.Date).FirstOrDefault()})
    .Select(__h__TransparentIdentifier0 => new {AccountId = __h__TransparentIdentifier0.clickAllocation.AccountId, Date = __h__TransparentIdentifier0.clickAllocation.Date, Id = __h__TransparentIdentifier0.clickAllocation.Id, Key = __h__TransparentIdentifier0.clickAllocation.Key, LastSavedDate = __h__TransparentIdentifier0.clickAllocation.LastSavedDate, LastSavedUser = __h__TransparentIdentifier0.clickAllocation.LastSavedUser, OrderNumber = __h__TransparentIdentifier0.clickAllocation.OrderNumber, PurchaseDate = __h__TransparentIdentifier0.clickAllocation.PurchaseDate, PurchaseOrderNumber = __h__TransparentIdentifier0.clickAllocation.PurchaseOrderNumber, Quantity = __h__TransparentIdentifier0.clickAllocation.Quantity, ReorderQuantity = __h__TransparentIdentifier0.clickAllocation.ReorderQuantity, Type = __h__TransparentIdentifier0.clickAllocation.Type})")
                                                }}));

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }

        [Fact]
        public void CanIndexWithNoErrors_Datetime()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var json = commands.ParseJson(LinuxTestUtils.Dos2Unix(@"{
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
}"));

                    commands.Put("test/1", null, json, new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "ClickAllocations" } });

                    json = commands.ParseJson(LinuxTestUtils.Dos2Unix(@"{
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
}"));

                    commands.Put("test/2", null, json, new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "ClickAllocations" } });
                }

                store.Maintenance.Send(new PutIndexesOperation(new[] {
                                                new IndexDefinition
                                                {
                                                    Name = "test",
                                                    Maps = {
                                                        LinuxTestUtils.Dos2Unix(@"docs.ClickAllocations
    .Select(doc => new {AccountId = doc.AccountId, Date = doc.Date, Id = Id(doc), Key = doc.Key, LastSavedDate = doc.LastSavedDate, LastSavedUser = doc.LastSavedUser, OrderNumber = doc.OrderNumber, PurchaseDate = doc.PurchaseDate, PurchaseOrderNumber = doc.PurchaseOrderNumber, Quantity = doc.Quantity, ReorderQuantity = doc.ReorderQuantity, Type = doc.Type})
") },
                                                    Reduce =
                                                        LinuxTestUtils.Dos2Unix(@"results
    .GroupBy(result => result.AccountId)
    .Select(a => new {a = a, clickAllocation = a.OrderByDescending(x => x.Date).FirstOrDefault()})
    .Select(__h__TransparentIdentifier0 => new {AccountId = __h__TransparentIdentifier0.clickAllocation.AccountId, Date = __h__TransparentIdentifier0.clickAllocation.Date, Id = __h__TransparentIdentifier0.clickAllocation.Id, Key = __h__TransparentIdentifier0.clickAllocation.Key, LastSavedDate = __h__TransparentIdentifier0.clickAllocation.LastSavedDate, LastSavedUser = __h__TransparentIdentifier0.clickAllocation.LastSavedUser, OrderNumber = __h__TransparentIdentifier0.clickAllocation.OrderNumber, PurchaseDate = __h__TransparentIdentifier0.clickAllocation.PurchaseDate, PurchaseOrderNumber = __h__TransparentIdentifier0.clickAllocation.PurchaseOrderNumber, Quantity = __h__TransparentIdentifier0.clickAllocation.Quantity, ReorderQuantity = __h__TransparentIdentifier0.clickAllocation.ReorderQuantity, Type = __h__TransparentIdentifier0.clickAllocation.Type})")
                                                }}));

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }
    }
}
