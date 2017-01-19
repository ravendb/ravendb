using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Server.NotificationCenter.Actions.Database;
using Raven.Server.NotificationCenter.Actions.Details;
using Raven.Server.NotificationCenter.Alerts;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Documents.Alerts
{
    public class BasicAlertsTest : RavenLowLevelTestBase
    {
        [Fact]
        public void Can_write_and_read_alert()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert = SampleAlert();

                database.NotificationCenter.Add(alert);

                IEnumerable<BlittableJsonReaderObject> alerts;
                using (database.NotificationCenter.GetAlerts(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(1, jsonAlerts.Count);

                    var readAlert = jsonAlerts[0];

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
                    Assert.Equal(alert.AlertId, readAlert[nameof(RaiseAlert.AlertId)].ToString());
                    Assert.Equal(alert.DismissedUntil, readAlert[nameof(RaiseAlert.DismissedUntil)]);
                }
            }
        }

        [Fact]
        public void Can_update_alert()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert1 = SampleAlert();

                database.NotificationCenter.Add(alert1);

                var alert2 = SampleAlert(customMessage: "updated");
                database.NotificationCenter.Add(alert2);

                IEnumerable<BlittableJsonReaderObject> alerts;
                using (database.NotificationCenter.GetAlerts(out alerts))
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
        public void Update_should_retain_dismissed_date()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert1 = SampleAlert();
                alert1.DismissedUntil = new DateTime(2014, 10, 2);

                database.NotificationCenter.Add(alert1);

                var alert2 = SampleAlert();
                database.NotificationCenter.Add(alert2);

                IEnumerable<BlittableJsonReaderObject> alerts;
                using (database.NotificationCenter.GetAlerts(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(1, jsonAlerts.Count);
                    var readAlert = jsonAlerts[0];

                    Assert.Equal(alert2.CreatedAt.GetDefaultRavenFormat(alert2.CreatedAt.Kind == DateTimeKind.Utc),
                        readAlert[nameof(RaiseAlert.CreatedAt)].ToString());

                    Assert.Equal(
                        alert1.DismissedUntil.Value.GetDefaultRavenFormat(alert1.DismissedUntil.Value.Kind == DateTimeKind.Utc),
                        readAlert[nameof(RaiseAlert.DismissedUntil)].ToString());
                }
            }
        }

        [Fact]
        public void Can_delete_alert()
        {
            using (var database = CreateDocumentDatabase())
            {
                var alert1 = SampleAlert();

                database.NotificationCenter.Add(alert1);

                database.NotificationCenter.DeleteAlert(alert1.AlertType, alert1.Key);


                IEnumerable<BlittableJsonReaderObject> alerts;
                using (database.NotificationCenter.GetAlerts(out alerts))
                {
                    var jsonAlerts = alerts.ToList();

                    Assert.Equal(0, jsonAlerts.Count);
                }
            }
        }

        private static RaiseAlert SampleAlert(string customMessage = null)
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