using System;
using Raven.Server.NotificationCenter.Actions.Details;
using Raven.Server.NotificationCenter.Alerts;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions.Database
{
    public class RaiseAlert : DatabaseAction, IAlert
    {
        private RaiseAlert()
        {
        }

        public AlertSeverity Severity { get; set; }

        public DatabaseAlertType AlertType { get; set; }

        public string Key { get; set; }

        public string Id => AlertUtil.CreateId(AlertType, Key);

        public DateTime? DismissedUntil { get; set; }

        public override DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Title)] = Title,
                [nameof(Message)] = Message,
                [nameof(Severity)] = Severity.ToString(),
                [nameof(Type)] = Type.ToString(),
                [nameof(Key)] = Key,
                [nameof(CreatedAt)] = CreatedAt,
                [nameof(DismissedUntil)] = DismissedUntil,
                [nameof(Details)] = Details?.ToJson()
            };
        }

        public static RaiseAlert Create(string title, string msg, DatabaseAlertType type, AlertSeverity severity, string key = null, IActionDetails details = null)
        {
            return new RaiseAlert
            {
                Title = title,
                Message = msg,
                Type = ActionType.Alert,
                AlertType = type,
                Severity = severity,
                Key = key,
                Details = details
            };
        }
    }
}