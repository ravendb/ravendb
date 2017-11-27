using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public class NotificationUpdated : Notification
    {
        private NotificationUpdated() : base(NotificationType.NotificationUpdated, "*")
        {
        }

        public override string Id => $"{Type}/{UpdateType}/{NotificationId}";

        public string NotificationId { get; private set; }

        public NotificationUpdateType UpdateType { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(NotificationId)] = NotificationId;
            result[nameof(Type)] = Type;
            result[nameof(UpdateType)] = UpdateType;

            return result;
        }

        public static NotificationUpdated Create(string id, NotificationUpdateType type)
        {
            return new NotificationUpdated
            {
                NotificationId = id,
                UpdateType = type
            };
        }
    }
}
