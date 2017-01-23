using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions
{
    public class NotificationDeleted : Action
    {
        private NotificationDeleted()
        {
            
        }

        public string ActionId { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(ActionId)] = ActionId;

            return result;
        }

        public static NotificationDeleted Create(string id)
        {
            return new NotificationDeleted
            {
                Type = ActionType.NotificationUpdate,
                ActionId = id
            };
        }
    }
}