using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public class AlertRaised : Notification
    {
        private AlertRaised(string database) : base(NotificationType.AlertRaised, database)
        {
        }
        
        public AlertType AlertType { get; private set; }

        public string Key { get; private set; }

        public override string Id => GetKey(AlertType, Key);
        
        public INotificationDetails Details { get; protected set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            
            json[nameof(Key)] = Key;
            json[nameof(AlertType)] = AlertType;
            json[nameof(Details)] = Details?.ToJson();

            return json;
        }

        public static AlertRaised Create(string database, string title, string msg, AlertType type, NotificationSeverity severity, string key = null, INotificationDetails details = null)
        {
            return new AlertRaised(database)
            {
                IsPersistent = true,
                Title = title,
                Message = msg,
                AlertType = type,
                Severity = severity,
                Key = key,
                Details = details
            };
        }

        public static string GetKey(AlertType alertType, string key)
        {
            return string.IsNullOrEmpty(key) ? $"{NotificationType.AlertRaised}/{alertType}" : $"{NotificationType.AlertRaised}/{alertType}/{key}";
        }
    }
}
