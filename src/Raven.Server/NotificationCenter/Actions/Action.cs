using System;
using Raven.Abstractions;
using Raven.Server.NotificationCenter.Actions.Details;
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

        public ActionType Type { get; private set; }

        public string Title { get; protected set; }

        public string Message { get; protected set; }

        public bool IsPersistent { get; protected set; }

        public DateTime? PostponedUntil { get; set; }

        public IActionDetails Details { get; protected set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(CreatedAt)] = CreatedAt,
                [nameof(Type)] = Type.ToString(),
                [nameof(Title)] = Title,
                [nameof(Message)] = Message,
                [nameof(IsPersistent)] = IsPersistent,
                [nameof(PostponedUntil)] = PostponedUntil,
                [nameof(Details)] = Details?.ToJson()
            };
        }
    }
}