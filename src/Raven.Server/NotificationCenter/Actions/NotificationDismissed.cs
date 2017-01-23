using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions
{
    public class NotificationDismissed : Action
    {
        private NotificationDismissed()
        {
        }

        public string ActionId { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(ActionId)] = ActionId;

            return result;
        }

        public static NotificationDismissed Create(string id)
        {
            return new NotificationDismissed
            {
                Type = ActionType.NotificationUpdate,
                ActionId = id
            };
        }
    }
}