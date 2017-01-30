using System;
using Raven.Abstractions;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public abstract class Notification
    {
        protected Notification(NotificationType type)
        {
            CreatedAt = SystemTime.UtcNow;
            Type = type;
        }

        public abstract string Id { get; }

        public DateTime CreatedAt { get; }

        public NotificationType Type { get; }

        public string Title { get; protected set; }

        public string Message { get; protected set; }

        public bool IsPersistent { get; protected set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(CreatedAt)] = CreatedAt,
                [nameof(Type)] = Type.ToString(),
                [nameof(Title)] = Title,
                [nameof(Message)] = Message,
                [nameof(IsPersistent)] = IsPersistent
            };
        }
    }
}