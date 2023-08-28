using System;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public sealed class AlertRaised : Notification
    {
        private AlertRaised(string database, DateTime? createdAt = null) 
            : base(NotificationType.AlertRaised, database, createdAt)
        {
        }
        
        public AlertType AlertType { get; private set; }

        public string Key { get; private set; }

        public override string Id => GetKey(AlertType, Key);
        
        public INotificationDetails Details { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            
            json[nameof(Key)] = Key;
            json[nameof(AlertType)] = AlertType;
            json[nameof(Details)] = Details?.ToJson();

            return json;
        }

        public static AlertRaised FromJson(string key, BlittableJsonReaderObject json, INotificationDetails details = null)
        {
            json.TryGet(nameof(Database), out string database);
            json.TryGet(nameof(AlertType), out AlertType alertType);
            json.TryGet(nameof(CreatedAt), out DateTime createdAt);
            json.TryGet(nameof(Message), out string message);
            json.TryGet(nameof(Severity), out NotificationSeverity notificationSeverity);
            json.TryGet(nameof(Title), out string title);

            return new AlertRaised(database, createdAt)
            {
                IsPersistent = true,
                Title = title,
                Message = message,
                AlertType = alertType,
                Severity = notificationSeverity,
                Key = key,
                Details = details
            };
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
