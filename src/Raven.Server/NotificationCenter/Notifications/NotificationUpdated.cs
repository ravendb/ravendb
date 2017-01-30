using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public class NotificationUpdated : Notification
    {
        private NotificationUpdated() : base(NotificationType.NotificationUpdated)
        {
        }

        public override string Id => $"{Type}/{UpdateType}/{ActionId}";

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