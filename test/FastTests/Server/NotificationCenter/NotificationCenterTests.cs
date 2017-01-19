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
                    database.NotificationCenter.Add(GetSampleDatabaseAlert());
                }

                Assert.Equal(1, actions.Count);
            }
        }

        [Fact]
        public void Persistent_action_is_stored_and_can_be_read()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert = GetSampleDatabaseAlert();

                Assert.True(alert.IsPersistent);

                database.NotificationCenter.Add(alert);

                IEnumerable<BlittableJsonReaderObject> actions;
                using (database.NotificationCenter.GetStored(out actions))
                {
                    var jsonAlerts = actions.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    var readAlert = jsonAlerts[0];

                    Assert.Equal(alert.Id, readAlert[nameof(RaiseAlert.Id)].ToString());
                    Assert.Equal(alert.CreatedAt.GetDefaultRavenFormat(alert.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert[nameof(RaiseAlert.CreatedAt)].ToString());
                    Assert.Equal(alert.Type.ToString(), readAlert[nameof(RaiseAlert.Type)].ToString());
                    Assert.Equal(alert.Title, readAlert[nameof(RaiseAlert.Title)].ToString());
                    Assert.Equal(alert.Message, readAlert[nameof(RaiseAlert.Message)].ToString());

                    Assert.Equal(((ExceptionDetails)alert.Details).Exception,
                        ((BlittableJsonReaderObject)readAlert[nameof(RaiseAlert.Details)])[nameof(ExceptionDetails.Exception)].ToString());

                    Assert.Equal(alert.Severity.ToString(), readAlert[nameof(RaiseAlert.Severity)].ToString());
                    Assert.Equal(alert.AlertType.ToString(), readAlert[nameof(RaiseAlert.AlertType)].ToString());
                    Assert.Equal(alert.Key, readAlert[nameof(RaiseAlert.Key)].ToString());
                    Assert.Equal(alert.DismissedUntil, readAlert[nameof(RaiseAlert.DismissedUntil)]);
                }
            }
        }

        [Fact]
        public void Can_update_alert()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert1 = GetSampleDatabaseAlert();

                database.NotificationCenter.Add(alert1);

                var alert2 = GetSampleDatabaseAlert(customMessage: "updated");
                database.NotificationCenter.Add(alert2);

                IEnumerable<BlittableJsonReaderObject> alerts;
                using (database.NotificationCenter.GetStored(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(1, jsonAlerts.Count);

                    var readAlert = jsonAlerts[0];

                    Assert.Equal(alert2.CreatedAt.GetDefaultRavenFormat(alert2.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert[nameof(RaiseAlert.CreatedAt)].ToString());
                    Assert.Equal(alert2.Message, readAlert[nameof(RaiseAlert.Message)].ToString());
                }
            }
        }

        [Fact]
        public void Repeated_alert_should_retain_dismiss_until_date()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert1 = GetSampleDatabaseAlert();
                database.NotificationCenter.Add(alert1);

                var dismissUntil = SystemTime.UtcNow.AddDays(1);
                database.NotificationCenter.DismissUntil(alert1.Id, dismissUntil);

                var alert2 = GetSampleDatabaseAlert();
                database.NotificationCenter.Add(alert2);

                IEnumerable<BlittableJsonReaderObject> alerts;
                using (database.NotificationCenter.GetStored(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    var readAlert = jsonAlerts[0];

                    Assert.Equal(alert2.CreatedAt.GetDefaultRavenFormat(alert2.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert[nameof(RaiseAlert.CreatedAt)].ToString());

                    Assert.Equal(
                        dismissUntil.GetDefaultRavenFormat(dismissUntil.Kind == DateTimeKind.Utc),
                        readAlert[nameof(RaiseAlert.DismissedUntil)].ToString());
                }
            }
        }

        [Fact]
        public void Can_dismiss_and_get_notified_about_it()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert = GetSampleDatabaseAlert();
                database.NotificationCenter.Add(alert);

                var dismissUntil = SystemTime.UtcNow.AddDays(1);

                var actions = new AsyncQueue<Action>();
                using (database.NotificationCenter.TrackActions(actions))
                {
                    database.NotificationCenter.DismissUntil(alert.Id, dismissUntil);
                }

                Assert.Equal(1, actions.Count);
                var notification = actions.DequeueAsync().Result as NotificationDismissed;
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
                        readAlert[nameof(RaiseAlert.CreatedAt)].ToString());

                    Assert.Equal(
                        dismissUntil.GetDefaultRavenFormat(dismissUntil.Kind == DateTimeKind.Utc),
                        readAlert[nameof(RaiseAlert.DismissedUntil)].ToString());
                }
            }
        }

        [Fact]
        public void Can_delete_action_and_get_notified_about_it()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert = GetSampleDatabaseAlert();

                database.NotificationCenter.Add(alert);

                var actions = new AsyncQueue<Action>();
                using (database.NotificationCenter.TrackActions(actions))
                {
                    database.NotificationCenter.Delete(alert.Id);

                    IEnumerable<BlittableJsonReaderObject> alerts;
                    using (database.NotificationCenter.GetStored(out alerts))
                    {
                        var jsonAlerts = alerts.ToList();

                        Assert.Equal(0, jsonAlerts.Count);
                    }
                }

                Assert.Equal(1, actions.Count);
                var notification = actions.DequeueAsync().Result as NotificationDeleted;
                Assert.NotNull(notification);
                Assert.Equal(alert.Id, notification.ActionId);
            }
        }

        private static RaiseAlert GetSampleDatabaseAlert(string customMessage = null)
        {
            return RaiseAlert.Create(
                "title",
                customMessage ?? "Alert #1",
                0, //use any type
                AlertSeverity.Info,
                key: "Key",
                details: new ExceptionDetails(new Exception("Error message")));
        }
    }
}