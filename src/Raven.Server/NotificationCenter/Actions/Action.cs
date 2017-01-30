using System;
using Raven.Abstractions;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions
{
    public abstract class Action
    {
        protected Action(ActionType type)
        {
            CreatedAt = SystemTime.UtcNow;
            Type = type;
        }

        public abstract string Id { get; }

        public DateTime CreatedAt { get; }

        public ActionType Type { get; }

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