using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
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

                using (database.NotificationCenter.TrackActions(actions))
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

                IEnumerable<BlittableJsonReaderObject> actions;
                using (database.NotificationCenter.GetStored(out actions))
                {
                    var jsonAlerts = actions.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    var readAlert = jsonAlerts[0];

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
                    Assert.Equal(alert.PostponedUntil, readAlert[nameof(AlertRaised.PostponedUntil)]);
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

                IEnumerable<BlittableJsonReaderObject> alerts;
                using (database.NotificationCenter.GetStored(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(1, jsonAlerts.Count);

                    var readAlert = jsonAlerts[0];

                    Assert.Equal(alert2.CreatedAt.GetDefaultRavenFormat(alert2.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert[nameof(AlertRaised.CreatedAt)].ToString());
                    Assert.Equal(alert2.Message, readAlert[nameof(AlertRaised.Message)].ToString());
                }
            }
        }

        [Fact]
        public void Repeated_alert_should_retain_dismiss_until_date()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert1 = GetSampleAlert();
                database.NotificationCenter.Add(alert1);

                var dismissUntil = SystemTime.UtcNow.AddDays(1);
                database.NotificationCenter.Postpone(alert1.Id, dismissUntil);

                var alert2 = GetSampleAlert();
                database.NotificationCenter.Add(alert2);

                IEnumerable<BlittableJsonReaderObject> alerts;
                using (database.NotificationCenter.GetStored(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    var readAlert = jsonAlerts[0];

                    Assert.Equal(alert2.CreatedAt.GetDefaultRavenFormat(alert2.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert[nameof(AlertRaised.CreatedAt)].ToString());

                    Assert.Equal(
                        dismissUntil.GetDefaultRavenFormat(dismissUntil.Kind == DateTimeKind.Utc),
                        readAlert[nameof(AlertRaised.PostponedUntil)].ToString());
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

                var dismissUntil = SystemTime.UtcNow.AddDays(1);

                var actions = new AsyncQueue<Action>();
                using (database.NotificationCenter.TrackActions(actions))
                {
                    database.NotificationCenter.Postpone(alert.Id, dismissUntil);
                }

                Assert.Equal(1, actions.Count);
                var notification = actions.DequeueAsync().Result as NotificationPostponed;
                Assert.NotNull(notification);
                Assert.Equal(alert.Id, notification.ActionId);
                Assert.Equal(dismissUntil, notification.NotificationDismissedUntil);

                IEnumerable<BlittableJsonReaderObject> alerts;
                using (database.NotificationCenter.GetStored(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    var readAlert = jsonAlerts[0];

                    Assert.Equal(alert.CreatedAt.GetDefaultRavenFormat(alert.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert[nameof(AlertRaised.CreatedAt)].ToString());

                    Assert.Equal(
                        dismissUntil.GetDefaultRavenFormat(dismissUntil.Kind == DateTimeKind.Utc),
                        readAlert[nameof(AlertRaised.PostponedUntil)].ToString());
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
                using (database.NotificationCenter.TrackActions(actions))
                {
                    database.NotificationCenter.Dismiss(alert.Id);

                    IEnumerable<BlittableJsonReaderObject> alerts;
                    using (database.NotificationCenter.GetStored(out alerts))
                    {
                        var jsonAlerts = alerts.ToList();

                        Assert.Equal(0, jsonAlerts.Count);
                    }
                }

                Assert.Equal(1, actions.Count);
                var notification = actions.DequeueAsync().Result as NotificationDismissed;
                Assert.NotNull(notification);
                Assert.Equal(alert.Id, notification.ActionId);
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