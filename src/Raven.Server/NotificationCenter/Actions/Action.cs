using System;
using Raven.Abstractions;
using Raven.Server.NotificationCenter.Actions.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions
{
    public abstract class Action
    {
        protected Action()
        {
            CreatedAt = SystemTime.UtcNow;
        }

        public DateTime CreatedAt { get; set; }

        public ActionType Type { get; protected set; }

        public string Title { get; protected set; }

        public string Message { get; protected set; }

        public IActionDetails Details { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Title)] = Title,
                [nameof(Message)] = Message,
                [nameof(Type)] = Type.ToString(),
                [nameof(CreatedAt)] = CreatedAt,
                [nameof(Details)] = Details?.ToJson()
            };
        }
    }
}