using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Actions;
using Raven.Server.NotificationCenter.Actions.Details;
using Raven.Server.NotificationCenter.Alerts;
using Sparrow.Collections;
using Sparrow.Json;
using Xunit;
using Action = Raven.Server.NotificationCenter.Actions.Action;

namespace FastTests.Server.NotificationCenter
{
    public class NotificationCenterTests : RavenLowLevelTestBase
    {
        [Fact]
        public void Should_get_notification()
        {
            using (var database = CreateDocumentDatabase())
            {
                var actions = new AsyncQueue<Action>();
                var writer = new TestWebSockerWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    database.NotificationCenter.Add(GetSampleAlert());
                }

                Assert.Equal(1, actions.Count);
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

                IEnumerable<ActionTableValue> actions;
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

                IEnumerable<ActionTableValue> alerts;
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
                var alert1 = GetSampleAlert();
                database.NotificationCenter.Add(alert1);

                var postponeUntil = SystemTime.UtcNow.AddDays(1);
                database.NotificationCenter.Postpone(alert1.Id, postponeUntil);

                var alert2 = GetSampleAlert();
                database.NotificationCenter.Add(alert2);

                IEnumerable<ActionTableValue> alerts;
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
                var alert = GetSampleAlert();
                database.NotificationCenter.Add(alert);

                var postponeUntil = SystemTime.UtcNow.AddDays(1);

                var actions = new AsyncQueue<Action>();
                var writer = new TestWebSockerWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    database.NotificationCenter.Postpone(alert.Id, postponeUntil);
                }

                Assert.Equal(1, actions.Count);
                var notification = actions.DequeueAsync().Result as NotificationUpdated;
                Assert.NotNull(notification);
                Assert.Equal(alert.Id, notification.ActionId);
                Assert.Equal(NotificationUpdateType.Postponed, notification.UpdateType);

                IEnumerable<ActionTableValue> alerts;
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
                var alert = GetSampleAlert();

                database.NotificationCenter.Add(alert);

                var actions = new AsyncQueue<Action>();
                var writer = new TestWebSockerWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    database.NotificationCenter.Dismiss(alert.Id);

                    IEnumerable<ActionTableValue> alerts;
                    using (database.NotificationCenter.GetStored(out alerts))
                    {
                        var jsonAlerts = alerts.ToList();

                        Assert.Equal(0, jsonAlerts.Count);
                    }
                }

                Assert.Equal(1, actions.Count);
                var notification = actions.DequeueAsync().Result as NotificationUpdated;
                Assert.NotNull(notification);
                Assert.Equal(alert.Id, notification.ActionId);
                Assert.Equal(NotificationUpdateType.Dismissed, notification.UpdateType);
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

                database.NotificationCenter.Dismiss(alert1.Id);
                Assert.Equal(1, database.NotificationCenter.GetAlertCount());

                database.NotificationCenter.Dismiss(alert2.Id);
                Assert.Equal(0, database.NotificationCenter.GetAlertCount());
            }
        }

        [Fact]
        public void Can_filter_out_postponed_actions()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert = GetSampleAlert();

                database.NotificationCenter.Add(alert);

                var actions = new AsyncQueue<Action>();
                var writer = new TestWebSockerWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    var postponeUntil = SystemTime.UtcNow.AddDays(1);

                    database.NotificationCenter.Postpone(alert.Id, postponeUntil);

                    IEnumerable<ActionTableValue> alerts;
                    using (database.NotificationCenter.GetStored(out alerts, postponed: false))
                    {
                        var jsonAlerts = alerts.ToList();

                        Assert.Equal(0, jsonAlerts.Count);
                    }
                }
            }
        }


        [Fact]
        public void Persisten_actions_are_returned_in_creation_order()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert1 = GetSampleAlert();
                var alert2 = GetSampleAlert(customKey: "aaaaaaa");

                database.NotificationCenter.Add(alert1);
                database.NotificationCenter.Add(alert2);

                IEnumerable<ActionTableValue> alerts;
                using (database.NotificationCenter.GetStored(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(2, jsonAlerts.Count);
                    Assert.Equal(alert1.Id, jsonAlerts[0].Json[nameof(Action.Id)].ToString());
                    Assert.Equal(alert2.Id, jsonAlerts[1].Json[nameof(Action.Id)].ToString());
                }
            }
        }

        [Fact]
        public void Should_send_postponed_notification_when_postpone_date_reached()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert = GetSampleAlert();
                database.NotificationCenter.Add(alert);

                var actions = new AsyncQueue<Action>();
                var writer = new TestWebSockerWriter();

                using (database.NotificationCenter.TrackActions(actions, writer))
                {
                    database.NotificationCenter.Postpone(alert.Id, SystemTime.UtcNow.AddMilliseconds(100));

                    var posponed = actions.DequeueAsync().Result as NotificationUpdated;

                    Assert.NotNull(posponed);
                    Assert.Equal(NotificationUpdateType.Postponed, posponed.UpdateType);

                    Assert.True(SpinWait.SpinUntil(() => writer.SentNotifications.Count == 1, TimeSpan.FromSeconds(1)));

                    Assert.Equal(alert.Id, writer.SentNotifications[0]);
                }
            }
        }

        private class TestWebSockerWriter : IWebsocketWriter
        {
            public List<string> SentNotifications { get; } = new List<string>();

            public Task WriteToWebSocket<TNotification>(TNotification notification)
            {
                var blittable = notification as BlittableJsonReaderObject;

                SentNotifications.Add(blittable[nameof(Action.Id)].ToString());

                return Task.CompletedTask;
            }
        }

        private static AlertRaised GetSampleAlert(string customMessage = null, string customKey = null)
        {
            return AlertRaised.Create(
                "title",
                customMessage ?? "Alert #1",
                0, //use any type
                AlertSeverity.Info,
                key: customKey ?? "Key",
                details: new ExceptionDetails(new Exception("Error message")));
        }
    }
}