using System;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public abstract class Notification
    {
        public const string ServerWide = null;

        public const string AllDatabases = "*";

        protected Notification(NotificationType type, string database)
        {
            CreatedAt = SystemTime.UtcNow;
            Type = type;
            Database = database;
        }

        public abstract string Id { get; }

        public DateTime CreatedAt { get; }

        public NotificationType Type { get; }

        /// <summary>
        /// The database this notification applies to
        /// null - server level
        /// * - applies to all dbs
        /// anything else - the db this applies to
        /// </summary>
        public string Database { get; }

        public string Title { get; protected set; }

        public string Message { get; protected set; }

        public bool IsPersistent { get; protected set; }
        
        public NotificationSeverity Severity { get; protected set; } = NotificationSeverity.None;

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(CreatedAt)] = CreatedAt,
                [nameof(Type)] = Type.ToString(),
                [nameof(Title)] = Title,
                [nameof(Message)] = Message,
                [nameof(Severity)] = Severity,
                [nameof(IsPersistent)] = IsPersistent,
                [nameof(Database)] = Database
            };
        }
    }
}
