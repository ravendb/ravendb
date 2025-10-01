using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
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
    [RavenTheory(RavenTestCategory.Indexes)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TestGetNotificationsEndpoint(bool typeAsNumber)
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
            await Indexes.WaitForIndexingAsync(store);

            //filter nonexisting one
            var notifications = await store.Maintenance.SendAsync(new GetNotificationsOperation(alertType: NotificationTypeParameter.PerformanceHint, testAsNumber: typeAsNumber));
            Assert.Empty(notifications.Results);

            //Notification should be raised soon, let's get it as json from get
            Assert.Equal(1, await WaitForValueAsync(async () => (notifications = await store.Maintenance.SendAsync(new GetNotificationsOperation(alertType: NotificationTypeParameter.Alert, testAsNumber: typeAsNumber))).TotalResults, expectedVal: 1));

            //paging test:
            {
                var notificationsTemp = await store.Maintenance.SendAsync(new GetNotificationsOperation(pageSize: 0, alertType: NotificationTypeParameter.Alert, testAsNumber: typeAsNumber));
                Assert.Equal(1, notificationsTemp.TotalResults);
                Assert.Empty(notificationsTemp.Results);

                notificationsTemp = await store.Maintenance.SendAsync(new GetNotificationsOperation(pageSize: 1, pageStart: 1, alertType: NotificationTypeParameter.Alert, testAsNumber: typeAsNumber));
                Assert.Equal(1, notificationsTemp.TotalResults);
                Assert.Empty(notificationsTemp.Results);

                notificationsTemp = await store.Maintenance.SendAsync(new GetNotificationsOperation(pageSize: 1, pageStart: 0, alertType: NotificationTypeParameter.Alert, testAsNumber: typeAsNumber));
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
            notifications = await store.Maintenance.SendAsync(new GetNotificationsOperation(alertType: NotificationTypeParameter.Alert, testAsNumber: typeAsNumber));
            Assert.Equal(0, notifications.Results.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Monitoring)]
    [InlineData(NotificationType.AlertRaised, true)]
    [InlineData(NotificationType.AlertRaised, false)]
    [InlineData(NotificationType.PerformanceHint, true)]
    [InlineData(NotificationType.PerformanceHint, false)]
    public async Task TestGetNotificationsEndpointPaging(NotificationType notificationType, bool flagAsInt)
    {
        using var store = GetDocumentStore();
        var db = await GetDatabase(store.Database);
        NotificationTypeParameter notificationTypeParameter = notificationType switch
        {
            NotificationType.PerformanceHint => NotificationTypeParameter.PerformanceHint,
            NotificationType.AlertRaised => NotificationTypeParameter.Alert,
            _ => NotificationTypeParameter.All
        };

        var notificationsQueue = new AsyncQueue<DynamicJsonValue>();
        using (db.NotificationCenter.TrackActions(notificationsQueue, null))
        {
            for (var id = 0; id < 10; ++id)
            {
                Notification notificationEndpointDto = id % 2 == 0
                    ? AlertRaised.Create("test", id.ToString(), id.ToString(), (AlertType)id, NotificationSeverity.Info)
                    : PerformanceHint.Create("test", id.ToString(), id.ToString(), (PerformanceHintType)(id % 5), NotificationSeverity.Info, "indexing");
                db.NotificationCenter.Add(notificationEndpointDto);
            }

            var result = store.Maintenance.Send(new GetNotificationsOperation(pageSize: 10, alertType: notificationTypeParameter, testAsNumber: flagAsInt));
            NotificationEndpointDto copyForAssertion = result;
            Assert.Equal(5, result.Results.Count(x => x.Type == notificationType));

            result = store.Maintenance.Send(new GetNotificationsOperation(pageSize: 0, alertType: notificationTypeParameter, testAsNumber: flagAsInt));
            Assert.Equal(5, result.TotalResults);
            Assert.Empty(result.Results);

            //paging
            {
                result = store.Maintenance.Send(new GetNotificationsOperation(pageStart: 0, pageSize: 2, alertType: notificationTypeParameter, testAsNumber: flagAsInt));
                Assert.Equal(5, result.TotalResults);
                Assert.Equal(2, result.Results.Count);
                Assert.Equal(copyForAssertion.Results[0].Title, result.Results[0].Title);
                Assert.Equal(copyForAssertion.Results[1].Title, result.Results[1].Title);

                result = store.Maintenance.Send(new GetNotificationsOperation(pageStart: 2, pageSize: 2, alertType: notificationTypeParameter, testAsNumber: flagAsInt));
                Assert.Equal(5, result.TotalResults);
                Assert.Equal(2, result.Results.Count);
                Assert.Equal(copyForAssertion.Results[2].Title, result.Results[0].Title);
                Assert.Equal(copyForAssertion.Results[3].Title, result.Results[1].Title);


                result = store.Maintenance.Send(new GetNotificationsOperation(pageStart: 4, pageSize: 2, alertType: notificationTypeParameter, testAsNumber: flagAsInt));
                Assert.Equal(5, result.TotalResults);
                Assert.Equal(1, result.Results.Count);
                Assert.Equal(copyForAssertion.Results[4].Title, result.Results[0].Title);


                result = store.Maintenance.Send(new GetNotificationsOperation(pageStart: 6, pageSize: 2, alertType: notificationTypeParameter, testAsNumber: flagAsInt));
                Assert.Equal(5, result.TotalResults);
                Assert.Equal(0, result.Results.Count);
            }

            result = store.Maintenance.Send(new GetNotificationsOperation(pageStart: 0, pageSize: 12, testAsNumber: flagAsInt));
            Assert.Equal(10, result.TotalResults);
            Assert.Equal(10, result.Results.Count);

            result = store.Maintenance.Send(new GetNotificationsOperation(pageSize: 0, testAsNumber: flagAsInt));
            Assert.Equal(10, result.TotalResults);
            Assert.Equal(0, result.Results.Count);

            //Flags
            result = store.Maintenance.Send(new GetNotificationsOperation(pageStart: 0, pageSize: 12, alertType: NotificationTypeParameter.Alert | NotificationTypeParameter.PerformanceHint, testAsNumber: flagAsInt));
            Assert.Equal(10, result.TotalResults);
            Assert.Equal(10, result.Results.Count);

            //Exception
            var ex = Assert.ThrowsAny<Exception>(() => store.Maintenance.Send(new GetNotificationsOperation(alertType: NotificationTypeParameter.NonExistingFlag, testAsNumber: flagAsInt)));
            Assert.Contains("Accepted values for type parameter are: [Alert, PerformanceHint]", ex.Message);
            Assert.Contains("BadRequestException", ex.Message);
        }
    }

    public RavenDB_22176(ITestOutputHelper output) : base(output)
    {
    }

    // ReSharper disable once ClassNeverInstantiated.Local
    private class SerializableNotification : Notification
    {
        public SerializableNotification(NotificationType type, string database) : base(type, database)
        {
        }

        public override string Id { get; }
    }

    private class NotificationEndpointDto
    {
        public int TotalResults { get; set; }
        public List<SerializableNotification> Results { get; set; }
    }

    [Flags]
    private enum NotificationTypeParameter : short
    {
        All = 0,
        Alert = 1,
        PerformanceHint = 1 << 1,
        NonExistingFlag = 1 << 2,
    }

    private class GetNotificationsOperation : IMaintenanceOperation<NotificationEndpointDto>
    {
        private readonly NotificationTypeParameter _alertType;
        private readonly bool _testAsNumber;
        private readonly bool _includeDefaultFilter;
        private readonly int? _pageStart;
        private readonly int? _pageSize;

        public GetNotificationsOperation(int? pageStart = null, int? pageSize = null, NotificationTypeParameter alertType = NotificationTypeParameter.All, bool testAsNumber = false, bool includeDefaultFilter = false)
        {
            _pageStart = pageStart;
            _pageSize = pageSize;
            _alertType = alertType;
            _testAsNumber = testAsNumber;
            _includeDefaultFilter = includeDefaultFilter;
        }

        public RavenCommand<NotificationEndpointDto> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetNotificationsCommand(_alertType, _pageStart, _pageSize, _testAsNumber, _includeDefaultFilter);
        }

        private class GetNotificationsCommand : RavenCommand<NotificationEndpointDto>
        {
            private readonly NotificationTypeParameter _alertType;
            private readonly int? _pageStart;
            private readonly int? _pageSize;
            private readonly bool _typeAsNumber;
            private readonly bool _includeDefaultFilter;


            public GetNotificationsCommand(NotificationTypeParameter alertType, int? pageStart, int? pageSize, bool typeAsNumber, bool includeDefaultFilter)
            {
                _alertType = alertType;
                _pageStart = pageStart;
                _pageSize = pageSize;
                _typeAsNumber = typeAsNumber;
                _includeDefaultFilter = includeDefaultFilter;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/notifications?";

                if (_alertType != NotificationTypeParameter.All || _includeDefaultFilter)
                {
                    var type = _typeAsNumber
                        ? ((short)_alertType).ToString()
                        : UrlEncode(_alertType.ToString());

                    url += $"&type={type}";
                }

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

                Result = DocumentConventions.DefaultForServer.Serialization.DeserializeEntityFromBlittable<NotificationEndpointDto>(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
