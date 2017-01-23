using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions
{
    public class NotificationPostponed : Action
    {
        private NotificationPostponed()
        {
        }

        public string ActionId { get; private set; }

        public DateTime NotificationDismissedUntil { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(ActionId)] = ActionId;

            return result;
        }

        public static NotificationPostponed Create(string id, DateTime until)
        {
            return new NotificationPostponed
            {
                Type = ActionType.NotificationUpdate,
                ActionId = id,
                NotificationDismissedUntil = until
            };
        }
    }
}