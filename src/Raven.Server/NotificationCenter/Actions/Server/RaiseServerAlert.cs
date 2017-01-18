using System;
using Raven.Server.NotificationCenter.Actions.Details;
using Raven.Server.NotificationCenter.Alerts;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions.Server
{
    public class RaiseServerAlert : ServerAction, IAlert
    {
        private RaiseServerAlert()
        {
        }

        public AlertSeverity Severity { get; set; }

        public ServerAlertType AlertType { get; set; }

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

        public static RaiseServerAlert Create(string title, string msg, ServerAlertType type, AlertSeverity severity, string key = null, IActionDetails details = null)
        {
            return new RaiseServerAlert
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