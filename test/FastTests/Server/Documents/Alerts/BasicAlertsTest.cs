using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Server.Alerts;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
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
            using (var _ctx = DocumentsOperationContext.ShortTermSingleUse(database))
            {
                var alert = SampleAlert();

                database.Alerts.AddAlert(alert);

                List<Alert> alerts;

                using (var ms = new MemoryStream())
                {
                    using (var writer = new BlittableJsonTextWriter(_ctx, ms))
                    {
                        database.Alerts.WriteAlerts(writer);
                    }

                    ms.Position = 0;

                    alerts = DeserializeAlerts(ms);
                }
                
                Assert.Equal(1, alerts.Count);
                var loadedAlert = alerts[0];
                Assert.Equal(alert.Type, loadedAlert.Type);
                Assert.Equal(alert.Message, loadedAlert.Message);
                Assert.Equal(alert.Severity, loadedAlert.Severity);
                Assert.Equal(alert.Read, loadedAlert.Read);
                Assert.Equal(alert.Key, loadedAlert.Key);
                Assert.IsType<ExceptionAlertContent>(loadedAlert.Content);
                var loadedContent = loadedAlert.Content as ExceptionAlertContent;
                var content = alert.Content as ExceptionAlertContent;
                Assert.Equal(content.Message, loadedContent.Message);
                Assert.Equal(content.Exception, loadedContent.Exception);
                Assert.Equal(alert.CreatedAt, loadedAlert.CreatedAt);
                Assert.Equal(alert.DismissedUntil, loadedAlert.DismissedUntil);
            }
        }

        [Fact]
        public void Can_update_alert()
        {
            using (var database = CreateDocumentDatabase())
            using (var _ctx = DocumentsOperationContext.ShortTermSingleUse(database))
            {
                var alert1 = SampleAlert();

                database.Alerts.AddAlert(alert1);

                var alert2 = SampleAlert();
                alert2.Message = "Updated message";
                database.Alerts.AddAlert(alert2);

                List<Alert> alerts;

                using (var ms = new MemoryStream())
                {
                    using (var writer = new BlittableJsonTextWriter(_ctx, ms))
                    {
                        database.Alerts.WriteAlerts(writer);
                    }

                    ms.Position = 0;

                    alerts = DeserializeAlerts(ms);
                }

                Assert.Equal(1, alerts.Count);
                var loadedAlert = alerts[0];
                Assert.Equal(alert2.Message, loadedAlert.Message);
            }
        }

        [Fact]
        public void Update_should_retain_dismissed_date()
        {
            using (var database = CreateDocumentDatabase())
            using (var _ctx = DocumentsOperationContext.ShortTermSingleUse(database))
            {
                var alert1 = SampleAlert();
                alert1.DismissedUntil = new DateTime(2014, 10, 2);

                database.Alerts.AddAlert(alert1);

                var alert2 = SampleAlert();
                database.Alerts.AddAlert(alert2);

                List<Alert> alerts;

                using (var ms = new MemoryStream())
                {
                    using (var writer = new BlittableJsonTextWriter(_ctx, ms))
                    {
                        database.Alerts.WriteAlerts(writer);
                    }

                    ms.Position = 0;

                    alerts = DeserializeAlerts(ms);
                }

                Assert.Equal(1, alerts.Count);
                var loadedAlert = alerts[0];
                Assert.Equal(alert1.DismissedUntil, loadedAlert.DismissedUntil);
            }
        }

        [Fact]
        public void Can_delete_alert()
        {
            using (var database = CreateDocumentDatabase())
            using (var _ctx = DocumentsOperationContext.ShortTermSingleUse(database))
            {
                var alert1 = SampleAlert();

                database.Alerts.AddAlert(alert1);

                database.Alerts.DeleteAlert(alert1.Type, alert1.Key);

                List<Alert> alerts;

                using (var ms = new MemoryStream())
                {
                    using (var writer = new BlittableJsonTextWriter(_ctx, ms))
                    {
                        database.Alerts.WriteAlerts(writer);
                    }

                    ms.Position = 0;

                    alerts = DeserializeAlerts(ms);
                }

                Assert.Equal(0, alerts.Count);
            }
        }

        private static Alert SampleAlert()
        {
            var alert = new Alert
            {
                Type = (AlertType)0, // use any type
                Message = "Alert #1",
                Severity = AlertSeverity.Info,
                Read = false,
                Key = null,
                Content = new ExceptionAlertContent
                {
                    Message = "Error message",
                    Exception = "Stack goes here"
                },
                CreatedAt = SystemTime.UtcNow,
                DismissedUntil = null
            };
            return alert;
        }

        private List<Alert> DeserializeAlerts(MemoryStream ms)
        {
            var conventions = new DocumentConvention();
            var alertList = RavenJToken.TryLoad(ms) as RavenJArray;
            return (List<Alert>) conventions.CreateSerializer().Deserialize(new RavenJTokenReader(alertList), typeof(List<Alert>));
        }
    }
}