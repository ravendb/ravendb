using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public sealed class RavenDB_22176 : RavenDB_17068_Base
{
    [RavenFact(RavenTestCategory.Indexes)]
    public async Task TestGetNotificationsEndpoint()
    {
        using var store = GetDocumentStore();
        var db = await GetDatabase(store.Database);

        var notificationsQueue = new AsyncQueue<DynamicJsonValue>();

        using (db.NotificationCenter.TrackActions(notificationsQueue, null))
        using (var session = store.OpenSession())
        {
            var c1 = new Cat() { Name = "Bingus" };
            session.Store(c1);
            var a1 = new Animal() { Name = "CoolAnimal" };
            session.Store(a1);
            var o1 = new Order() { AnimalId = c1.Id, Price = 22 };
            var o2 = new Order() { AnimalId = a1.Id, Price = 33 };
            session.Store(o1);
            session.Store(o2);
            session.SaveChanges();
            var index = new DummyIndex();
            await index.ExecuteAsync(store);
            Indexes.WaitForIndexing(store);

            //filter nonexisting one
            var notifications = store.Maintenance.Send(new GetNotifications(alertType: "OperationChanged"));
            Assert.Empty(notifications.Results);
            
            //Notification should be raised soon, let's get it as json from get
            notifications = store.Maintenance.Send(new GetNotifications(alertType: "AlertRaised"));
            Assert.Contains("We have detected usage of LoadDocument(doc, collectionName) where loaded document collection is different than given parameter", notifications.Results[0].ToString(), StringComparison.InvariantCultureIgnoreCase);

            //paging test:
            {
                var notificationsTemp = store.Maintenance.Send(new GetNotifications(pageSize: 0, alertType: "AlertRaised"));
                Assert.Equal(1, notificationsTemp.TotalResults);
                Assert.Empty(notificationsTemp.Results);
                
                notificationsTemp = store.Maintenance.Send(new GetNotifications(pageSize: 1, pageStart: 1, alertType: "AlertRaised"));
                Assert.Equal(0, notificationsTemp.TotalResults);
                Assert.Empty(notificationsTemp.Results);
                
                notificationsTemp = store.Maintenance.Send(new GetNotifications(pageSize: 1, pageStart: 0, alertType: "AlertRaised"));
                Assert.Equal(1, notificationsTemp.TotalResults);
                Assert.NotEmpty(notificationsTemp.Results);
            }
            
            Tuple<bool, DynamicJsonValue> alertRaised;
            do
            {
                alertRaised = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
            } while (alertRaised.Item2["Type"].ToString() != NotificationType.AlertRaised.ToString());

            var details = alertRaised.Item2[nameof(AlertRaised.Details)] as DynamicJsonValue;
            //Should be dequeued
            Assert_CheckIfMismatchesAreRemovedOnMatchingLoad(details);
            db.NotificationCenter.Dismiss("AlertRaised/MismatchedReferenceLoad/Indexing");
            notifications = store.Maintenance.Send(new GetNotifications(alertType: "AlertRaised"));
            Assert.Equal(0, notifications.Results.Count);
        }
    }

    public RavenDB_22176(ITestOutputHelper output) : base(output)
    {
    }

    private record Notification(int TotalResults, List<object> Results);

    private class GetNotifications : IMaintenanceOperation<Notification>
    {
        private string _alertType;
        private int? _pageStart;
        private int? _pageSize;
        
        public GetNotifications(int? pageStart = null, int? pageSize = null, string alertType = null)
        {
            _pageStart = pageStart;
            _pageSize = pageSize;
            _alertType = alertType;
        }

        public RavenCommand<Notification> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetNotificationsCommand(_alertType, _pageStart, _pageSize);
        }

        private class GetNotificationsCommand : RavenCommand<Notification>
        {
            private string _alertType;
            private int? _pageStart;
            private int? _pageSize;

            public GetNotificationsCommand(string alertType, int? pageStart, int? pageSize)
            {
                _alertType = alertType;
                _pageStart = pageStart;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/notifications";

                if (_alertType != null)
                    url += $"?type={_alertType}";

                if (_pageSize != null)
                    url += $"&pageSize={_pageSize.Value}";
                
                if (_pageStart != null)
                    url += $"&pageStart={_pageStart.Value}";
                
                return new HttpRequestMessage { Method = HttpMethod.Get };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonConvert.DeserializeObject<Notification>(response!.ToString());
            }

            public override bool IsReadRequest => true;
        }
    }
}
