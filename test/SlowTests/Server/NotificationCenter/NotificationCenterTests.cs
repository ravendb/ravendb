using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Operations;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Server.Collections;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.NotificationCenter
{
    public class NotificationCenterTests : RavenLowLevelTestBase
    {
        public NotificationCenterTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_get_notification()
        {
            using (var database = CreateDocumentDatabase())
            {
                var actions = new AsyncQueue<DynamicJsonValue>();
                var writer = new TestWebSocketWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    database.NotificationCenter.Add(GetSampleAlert());
                }

                Assert.Equal(1, actions.Count);
            }
        }

        [Fact]
        public void Request_latency_hints_are_stored_and_can_be_read()
        {
            using (var database = CreateDocumentDatabase())
            {
                database.NotificationCenter
                    .RequestLatency
                    .AddHint(5, "Query", "Query1");

                database.NotificationCenter
                    .RequestLatency
                    .AddHint(10, "Stream", "Query2");

                database.NotificationCenter
                    .RequestLatency
                    .AddHint(10, "Stream", "Query3");

                database.NotificationCenter
                    .RequestLatency
                    .UpdateRequestLatency(null);

                var storedRequestLatencyDetails = database.NotificationCenter.RequestLatency
                    .GetRequestLatencyDetails();
                Assert.Equal(2, storedRequestLatencyDetails.RequestLatencies.Count);
                Assert.Equal(1, storedRequestLatencyDetails.RequestLatencies["Query"].Count);

                Assert.Equal(2, storedRequestLatencyDetails.RequestLatencies["Stream"].Count);
            }
        }

        [Fact]
        public void Persistent_action_is_stored_and_can_be_read()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert = GetSampleAlert();

                Assert.True(alert.IsPersistent);

                database.NotificationCenter.Add(alert);

                IEnumerable<NotificationTableValue> actions;
                using (database.NotificationCenter.GetStored(out actions))
                {
                    var jsonAlerts = actions.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    var readAlert = jsonAlerts[0].Json;

                    Assert.Equal(alert.CreatedAt, jsonAlerts[0].CreatedAt);

                    Assert.Equal(alert.Id, readAlert[nameof(AlertRaised.Id)].ToString());
                    Assert.Equal(alert.CreatedAt.GetDefaultRavenFormat(alert.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert[nameof(AlertRaised.CreatedAt)].ToString());
                    Assert.Equal(alert.Type.ToString(), readAlert[nameof(AlertRaised.Type)].ToString());
                    Assert.Equal(alert.Title, readAlert[nameof(AlertRaised.Title)].ToString());
                    Assert.Equal(alert.Message, readAlert[nameof(AlertRaised.Message)].ToString());

                    Assert.Equal(((ExceptionDetails)alert.Details).Exception,
                        ((BlittableJsonReaderObject)readAlert[nameof(AlertRaised.Details)])[nameof(ExceptionDetails.Exception)].ToString());

                    Assert.Equal(alert.Severity.ToString(), readAlert[nameof(AlertRaised.Severity)].ToString());
                    Assert.Equal(alert.AlertType.ToString(), readAlert[nameof(AlertRaised.AlertType)].ToString());
                    Assert.Equal(alert.Key, readAlert[nameof(AlertRaised.Key)].ToString());
                }
            }
        }

        [Fact]
        public void Can_update_alert()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert1 = GetSampleAlert();

                database.NotificationCenter.Add(alert1);

                var alert2 = GetSampleAlert(customMessage: "updated");
                database.NotificationCenter.Add(alert2);

                IEnumerable<NotificationTableValue> alerts;
                using (database.NotificationCenter.GetStored(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(1, jsonAlerts.Count);

                    var readAlert = jsonAlerts[0];

                    Assert.Equal(alert2.CreatedAt, jsonAlerts[0].CreatedAt);

                    Assert.Equal(alert2.CreatedAt.GetDefaultRavenFormat(alert2.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert.Json[nameof(AlertRaised.CreatedAt)].ToString());

                    Assert.Equal(alert2.Message, readAlert.Json[nameof(AlertRaised.Message)].ToString());
                }
            }
        }

        [Fact]
        public void Repeated_alert_should_retain_postpone_until_date()
        {
            using (var database = CreateDocumentDatabase())
            {
                database.NotificationCenter.Options.DatabaseStatsThrottle = TimeSpan.MaxValue;

                var alert1 = GetSampleAlert();
                database.NotificationCenter.Add(alert1);

                var postponeUntil = SystemTime.UtcNow.AddDays(1);
                database.NotificationCenter.Postpone(alert1.Id, postponeUntil);

                var alert2 = GetSampleAlert();
                database.NotificationCenter.Add(alert2);

                IEnumerable<NotificationTableValue> alerts;
                using (database.NotificationCenter.GetStored(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    var readAlert = jsonAlerts[0];

                    Assert.Equal(alert2.CreatedAt, jsonAlerts[0].CreatedAt);
                    Assert.Equal(postponeUntil, jsonAlerts[0].PostponedUntil);

                    Assert.Equal(alert2.CreatedAt.GetDefaultRavenFormat(alert2.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert.Json[nameof(AlertRaised.CreatedAt)].ToString());
                }
            }
        }

        [Fact]
        public void Can_postpone_persistent_action_and_get_notified_about_it()
        {
            using (var database = CreateDocumentDatabase())
            {
                database.NotificationCenter.Options.DatabaseStatsThrottle = TimeSpan.MaxValue;

                var alert = GetSampleAlert();
                database.NotificationCenter.Add(alert);

                var postponeUntil = SystemTime.UtcNow.AddDays(1);

                var actions = new AsyncQueue<DynamicJsonValue>();
                var writer = new TestWebSocketWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    database.NotificationCenter.Postpone(alert.Id, postponeUntil);
                }

                Assert.Equal(1, actions.Count);
                var notification = actions.DequeueAsync().Result;
                Assert.NotNull(notification);
                Assert.Equal(alert.Id, notification[nameof(NotificationUpdated.NotificationId)]);
                Assert.Equal(NotificationUpdateType.Postponed, notification[nameof(NotificationUpdated.UpdateType)]);

                IEnumerable<NotificationTableValue> alerts;
                using (database.NotificationCenter.GetStored(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    var readAlert = jsonAlerts[0];

                    Assert.Equal(alert.CreatedAt, jsonAlerts[0].CreatedAt);

                    Assert.Equal(alert.CreatedAt.GetDefaultRavenFormat(alert.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert.Json[nameof(AlertRaised.CreatedAt)].ToString());

                    Assert.Equal(postponeUntil, jsonAlerts[0].PostponedUntil);
                }
            }
        }

        [Fact]
        public void Can_dismiss_persistent_action_and_get_notified_about_it()
        {
            using (var database = CreateDocumentDatabase())
            {
                database.NotificationCenter.Options.DatabaseStatsThrottle = TimeSpan.MaxValue;

                var alert = GetSampleAlert();

                database.NotificationCenter.Add(alert);

                var actions = new AsyncQueue<DynamicJsonValue>();
                var writer = new TestWebSocketWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    database.NotificationCenter.Dismiss(alert.Id);

                    IEnumerable<NotificationTableValue> alerts;
                    using (database.NotificationCenter.GetStored(out alerts))
                    {
                        var jsonAlerts = alerts.ToList();

                        Assert.Equal(0, jsonAlerts.Count);
                    }
                }

                Assert.Equal(1, actions.Count);
                var notification = actions.DequeueAsync().Result;
                Assert.NotNull(notification);
                Assert.Equal(alert.Id, notification[nameof(NotificationUpdated.NotificationId)]);
                Assert.Equal(NotificationUpdateType.Dismissed, notification[nameof(NotificationUpdated.UpdateType)]);
            }
        }

        [Fact]
        public void Can_get_alerts_count()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert1 = GetSampleAlert();
                var alert2 = GetSampleAlert(customKey: "different-key-will-force-different-id");

                database.NotificationCenter.Add(alert1);
                database.NotificationCenter.Add(alert2);

                Assert.Equal(2, database.NotificationCenter.GetAlertCount());
                Assert.Equal(0, database.NotificationCenter.GetPerformanceHintCount());

                database.NotificationCenter.Dismiss(alert1.Id);
                Assert.Equal(1, database.NotificationCenter.GetAlertCount());
                Assert.Equal(0, database.NotificationCenter.GetPerformanceHintCount());

                database.NotificationCenter.Dismiss(alert2.Id);
                Assert.Equal(0, database.NotificationCenter.GetAlertCount());
                Assert.Equal(0, database.NotificationCenter.GetPerformanceHintCount());
            }
        }

        [Fact]
        public void Can_get_performance_hint_count()
        {
            using (var database = CreateDocumentDatabase())
            {
                var hint1 = GetSamplePerformanceHint();
                var hint2 = GetSamplePerformanceHint(customSource: "different-key-will-force-different-id");

                database.NotificationCenter.Add(hint1);
                database.NotificationCenter.Add(hint2);

                Assert.Equal(0, database.NotificationCenter.GetAlertCount());
                Assert.Equal(2, database.NotificationCenter.GetPerformanceHintCount());

                database.NotificationCenter.Dismiss(hint1.Id);
                Assert.Equal(0, database.NotificationCenter.GetAlertCount());
                Assert.Equal(1, database.NotificationCenter.GetPerformanceHintCount());

                database.NotificationCenter.Dismiss(hint2.Id);
                Assert.Equal(0, database.NotificationCenter.GetAlertCount());
                Assert.Equal(0, database.NotificationCenter.GetPerformanceHintCount());
            }
        }

        [Fact]
        public void Can_filter_out_postponed_actions()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert = GetSampleAlert();

                database.NotificationCenter.Add(alert);

                var actions = new AsyncQueue<DynamicJsonValue>();
                var writer = new TestWebSocketWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    var postponeUntil = SystemTime.UtcNow.AddDays(1);

                    database.NotificationCenter.Postpone(alert.Id, postponeUntil);

                    IEnumerable<NotificationTableValue> alerts;
                    using (database.NotificationCenter.GetStored(out alerts, postponed: false))
                    {
                        var jsonAlerts = alerts.ToList();

                        Assert.Equal(0, jsonAlerts.Count);
                    }
                }
            }
        }

        [Fact]
        public void Persistent_actions_are_returned_in_creation_order()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert1 = GetSampleAlert();
                var alert2 = GetSampleAlert(customKey: "aaaaaaa");

                database.NotificationCenter.Add(alert1);
                database.NotificationCenter.Add(alert2);

                IEnumerable<NotificationTableValue> alerts;
                using (database.NotificationCenter.GetStored(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(2, jsonAlerts.Count);
                    Assert.Equal(alert1.Id, jsonAlerts[0].Json[nameof(Notification.Id)].ToString());
                    Assert.Equal(alert2.Id, jsonAlerts[1].Json[nameof(Notification.Id)].ToString());
                }
            }
        }

        [Fact]
        public void Should_send_postponed_notification_when_postpone_date_reached()
        {
            using (var database = CreateDocumentDatabase())
            {
                database.NotificationCenter.Options.DatabaseStatsThrottle = TimeSpan.MaxValue;

                var alert1 = GetSampleAlert(customKey: "alert-1");
                database.NotificationCenter.Add(alert1);

                var alert2 = GetSampleAlert(customKey: "alert-2");
                database.NotificationCenter.Add(alert2);

                var alert3 = GetSampleAlert(customKey: "alert-3");
                database.NotificationCenter.Add(alert3);

                var actions = new AsyncQueue<DynamicJsonValue>();
                var writer = new TestWebSocketWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    database.NotificationCenter.Postpone(alert1.Id, SystemTime.UtcNow.AddDays(1));
                    database.NotificationCenter.Postpone(alert2.Id, SystemTime.UtcNow.AddMilliseconds(100));
                    database.NotificationCenter.Postpone(alert3.Id, SystemTime.UtcNow.AddDays(1));

                    for (int i = 0; i < 2; i++)
                    {
                        var posponed = actions.DequeueAsync().Result;

                        Assert.NotNull(posponed);
                        Assert.Equal(NotificationUpdateType.Postponed, posponed[(nameof(NotificationUpdated.UpdateType))]);
                    }

                    Assert.True(SpinWait.SpinUntil(() => writer.SentNotifications.Count == 1, TimeSpan.FromSeconds(30)), $"Got: {writer.SentNotifications.Count}");

                    Assert.Equal(alert2.Id, writer.SentNotifications[0]);
                }
            }
        }

        [Fact]
        public async Task Duplicated_notification_should_not_arrive_before_postponed_until_date()
        {
            using (var database = CreateDocumentDatabase())
            {
                database.NotificationCenter.Options.DatabaseStatsThrottle = TimeSpan.MaxValue;

                var alert = GetSampleAlert();
                database.NotificationCenter.Add(alert);

                database.NotificationCenter.Postpone(alert.Id, SystemTime.UtcNow.AddDays(1));

                var actions = new AsyncQueue<DynamicJsonValue>();
                var writer = new TestWebSocketWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    database.NotificationCenter.Add(alert);
                    Assert.False((await actions.TryDequeueAsync(TimeSpan.FromMilliseconds(10))).Item1);

                    database.NotificationCenter.Add(alert);
                    Assert.False((await actions.TryDequeueAsync(TimeSpan.FromMilliseconds(10))).Item1);
                }
            }
        }

        [Fact]
        public void Should_persist_operation_if_result_requires_persistance()
        {
            using (var database = CreateDocumentDatabase())
            {
                database.NotificationCenter.Add(OperationChanged.Create(database.Name, 1, new Operations.OperationDescription(), new OperationState()
                {
                    Result = new PersistableResult()
                }, false));

                IEnumerable<NotificationTableValue> actions;
                using (database.NotificationCenter.GetStored(out actions))
                {
                    Assert.Equal(1, actions.Count());
                }
            }
        }

        [Fact]
        public void Can_postpone_notification_forever_then_next_notifictions_wont_be_sent()
        {
            using (var database = CreateDocumentDatabase())
            {
                database.NotificationCenter.Options.DatabaseStatsThrottle = TimeSpan.MaxValue;

                var alert = GetSampleAlert();

                database.NotificationCenter.Add(alert);

                var notifications = new AsyncQueue<DynamicJsonValue>();
                var writer = new TestWebSocketWriter();

                database.NotificationCenter.Postpone(alert.Id, DateTime.MaxValue);

                using (database.NotificationCenter.TrackActions(notifications, writer))
                {
                    database.NotificationCenter.Add(alert);

                    Assert.Equal(0, notifications.Count);
                }
            }
        }

        [Fact]
        public async Task Should_be_notified_about_changed_database_stats()
        {
            using (var database = CreateDocumentDatabase())
            {
                database.NotificationCenter.Options.DatabaseStatsThrottle = TimeSpan.FromMilliseconds(100);

                var actions = new AsyncQueue<DynamicJsonValue>();
                var writer = new TestWebSocketWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    var notification = await actions.TryDequeueAsync(TimeSpan.FromMilliseconds(5000));
                    Assert.True(notification.Item1);

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (var doc = context.ReadObject(new DynamicJsonValue
                    {
                        ["Foo"] = "Bar",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Collection] = "Foos"
                        }
                    }, ""))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            database.DocumentsStorage.Put(context, "foo/bar", null, doc);
                            tx.Commit();
                        }
                    }

                    notification = await actions.TryDequeueAsync(TimeSpan.FromMilliseconds(500));
                    Assert.True(notification.Item1);

                    var databaseStatsChanged = notification.Item2;

                    Assert.NotNull(databaseStatsChanged);

                    Assert.Equal(1L, databaseStatsChanged[nameof(DatabaseStatsChanged.CountOfDocuments)]);
                    Assert.Equal(0L, databaseStatsChanged[nameof(DatabaseStatsChanged.CountOfIndexes)]);
                    Assert.Equal(0L, databaseStatsChanged[nameof(DatabaseStatsChanged.CountOfStaleIndexes)]);
                    var collections = (databaseStatsChanged[nameof(DatabaseStatsChanged.ModifiedCollections)] as DynamicJsonArray);
                    Assert.Equal(1L, collections.Count);
                    Assert.Equal("Foos", (collections.First() as DynamicJsonValue)["Name"]);
                }
            }
        }

        [Fact]
        public void Should_skip_filtered_out_notifications()
        {
            // Filtering out two AlertType notifications and two entire notification types: PerformanceHint and DatabaseChanged
            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>();
            customSettings[RavenConfiguration.GetKey(x => x.Notifications.FilteredOutNotifications)] = $"{nameof(AlertType.LicenseManager_AGPL3)};" +
                                                                                                       $"{nameof(AlertType.RevisionsConfigurationNotValid)};" +
                                                                                                       $"{nameof(NotificationType.PerformanceHint)};" +
                                                                                                       $"{nameof(NotificationType.DatabaseChanged)};";
            var notifications = CreateSampleNotificationsForFilterOutTest();

            UseNewLocalServer(customSettings);

            using (var database = CreateDocumentDatabase())
            {
                foreach (var n in notifications)
                {
                    database.NotificationCenter.Add(n);
                }

                using (database.NotificationCenter.GetStored(out var alerts, postponed: false))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(4, jsonAlerts.Count); // 6 out of 10 are being filtered out
                }
            }
        }
        
        [LinuxFact]
        public void WhenActualSwapSmallerThenMinSwapConfigured_ShouldRaiseNotification()
        {
            var memoryInfoResult = MemoryInformation.GetMemoryInfo();
            var minSwapConfig = memoryInfoResult.TotalSwapSize + new Sparrow.Size(130, SizeUnit.Megabytes);
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.PerformanceHints.MinSwapSize)] = minSwapConfig.GetValue(SizeUnit.Megabytes).ToString()
            };

            UseNewLocalServer(customSettings);

            using var dis = Server.ServerStore.NotificationCenter.GetStored(out var alerts, postponed: false);
            var alertsList = alerts.ToList();

            var isLowSwapSizeRaised = alertsList.Any(a => a.Json[nameof(AlertRaised.AlertType)].ToString() == AlertType.LowSwapSize.ToString());
            Assert.True(isLowSwapSizeRaised, $"Actual swap {memoryInfoResult.TotalSwapSize}, min swap config {minSwapConfig}, total memory {memoryInfoResult.TotalPhysicalMemory}");
        }

        private static List<Notification> CreateSampleNotificationsForFilterOutTest()
        {
            return new List<Notification>
            {
                AlertRaised.Create(
                    null,
                    "DatabaseTopologyWarning",
                    "DatabaseTopologyWarning_MSG",
                    AlertType.DatabaseTopologyWarning,
                    NotificationSeverity.Info),
                DatabaseChanged.Create(null, DatabaseChangeType.Put), // filtered out, DatabaseChange
                AlertRaised.Create(
                    null,
                    "LicenseManager_AGPL3",
                    "LicenseManager_AGPL3_MSG",
                    AlertType.ClusterTransactionFailure,
                    NotificationSeverity.Info),
                AlertRaised.Create(
                    null,
                    "LicenseManager_AGPL3",
                    "LicenseManager_AGPL3_MSG",
                    AlertType.LicenseManager_AGPL3, // filtered out explicitly
                    NotificationSeverity.Info),
                AlertRaised.Create(
                    null,
                    "RevisionsConfigurationNotValid",
                    "RevisionsConfigurationNotValid_MSG",
                    AlertType.RevisionsConfigurationNotValid, // filtered out explicitly
                    NotificationSeverity.Info),
                AlertRaised.Create(
                    null,
                    "Certificates_ReplaceError",
                    "Certificates_ReplaceError_MSG",
                    AlertType.Certificates_ReplaceError,
                    NotificationSeverity.Info),
                PerformanceHint.Create(
                    null,
                    "SlowIO",
                    "SlowIO_MSG",
                    PerformanceHintType.SlowIO, // filtered out, PerformanceHint
                    NotificationSeverity.Info,
                    "test"),
                PerformanceHint.Create(
                    null,
                    "SqlEtl_SlowSql",
                    "SqlEtl_SlowSql_MSG",
                    PerformanceHintType.SqlEtl_SlowSql, // filtered out, PerformanceHint
                    NotificationSeverity.Info,
                    "test"),
                OperationChanged.Create(null,1, new Operations.OperationDescription(), new OperationState()
                {
                    Result = new PersistableResult()
                }, false),
                DatabaseChanged.Create(null, DatabaseChangeType.Delete) // filtered out, DatabaseChange
            };
        }

        protected class TestWebSocketWriter : IWebsocketWriter
        {
            public List<string> SentNotifications { get; } = new List<string>();

            public Task WriteToWebSocket<TNotification>(TNotification notification)
            {
                var blittable = notification as BlittableJsonReaderObject;

                SentNotifications.Add(blittable[nameof(Notification.Id)].ToString());

                return Task.CompletedTask;
            }
        }

        private static AlertRaised GetSampleAlert(string customMessage = null, string customKey = null)
        {
            return AlertRaised.Create(
                null,
                "title",
                customMessage ?? "Alert #1",
                0, //use any type
                NotificationSeverity.Info,
                key: customKey ?? "Key",
                details: new ExceptionDetails(new Exception("Error message")));
        }

        private static PerformanceHint GetSamplePerformanceHint(string customSource = null)
        {
            return PerformanceHint.Create("db", "title", "message", PerformanceHintType.None, NotificationSeverity.Info, source: customSource);
        }

        private class PersistableResult : IOperationResult
        {
            public string Message { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue();
            }

            public bool ShouldPersist => true;
        }

        public class TestRequestParams : IDictionary<string, string[]>, IQueryCollection
        {
            private readonly Dictionary<string, string[]> _data;

            public TestRequestParams()
            {
                _data = new Dictionary<string, string[]>();
            }

            public TestRequestParams(IDictionary<string, string[]> data)
            {
                _data = new Dictionary<string, string[]>(data);
            }

            IEnumerator<KeyValuePair<string, string[]>> IEnumerable<KeyValuePair<string, string[]>>.GetEnumerator()
            {
                return _data.GetEnumerator();
            }

            public IEnumerator<KeyValuePair<string, Microsoft.Extensions.Primitives.StringValues>> GetEnumerator()
            {
                return _data.Select(x => KeyValuePair.Create(x.Key, new Microsoft.Extensions.Primitives.StringValues(x.Value))).GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Add(string key, string[] value)
            {
                throw new NotImplementedException();
            }

            public bool ContainsKey(string key)
            {
                return _data.ContainsKey(key);
            }

            public bool Remove(string key)
            {
                return _data.Remove(key);
            }

            public bool TryGetValue(string key, out string[] value)
            {
                return _data.TryGetValue(key, out value);
            }

            string[] IDictionary<string, string[]>.this[string key]
            {
                get => _data[key];
                set => _data[key] = value;
            }

            public bool TryGetValue(string key, out Microsoft.Extensions.Primitives.StringValues value)
            {
                var success = _data.TryGetValue(key, out var val);
                value = new Microsoft.Extensions.Primitives.StringValues(val);
                return success;
            }

            public void Add(KeyValuePair<string, string[]> item)
            {
                _data.Add(item.Key, item.Value);
            }

            public void Clear()
            {
                _data.Clear();
            }

            public bool Contains(KeyValuePair<string, string[]> item)
            {
                return _data.Contains(item);
            }

            public void CopyTo(KeyValuePair<string, string[]>[] array, int arrayIndex)
            {
            }

            public bool Remove(KeyValuePair<string, string[]> item)
            {
                return _data.Remove(item.Key);
            }

            public int Count => _data.Count;
            public bool IsReadOnly => false;
            public ICollection<string> Keys => _data.Keys;
            public ICollection<string[]> Values => _data.Values;

            public Microsoft.Extensions.Primitives.StringValues this[string key] => new Microsoft.Extensions.Primitives.StringValues(_data[key]);
        }
    }
}
