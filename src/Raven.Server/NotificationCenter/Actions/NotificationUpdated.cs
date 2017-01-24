using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions
{
    public class NotificationUpdated : Action
    {
        private NotificationUpdated() : base(ActionType.NotificationUpdated)
        {
        }

        public string ActionId { get; private set; }

        public NotificationUpdateType UpdateType { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(ActionId)] = ActionId;
            result[nameof(Type)] = Type;

            return result;
        }

        public static NotificationUpdated Create(string id, NotificationUpdateType type)
        {
            return new NotificationUpdated
            {
                ActionId = id,
                UpdateType = type
            };
        }
    }
}