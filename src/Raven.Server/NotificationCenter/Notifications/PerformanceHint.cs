using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public sealed class PerformanceHint : Notification
    {
        private PerformanceHint(string database) : base(NotificationType.PerformanceHint, database)
        {
        }

        public PerformanceHintReason Reason { get; private set; }

        public string Source { get; private set; }

        public override string Id => GetKey(Reason, Source);

        public INotificationDetails Details { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(Source)] = Source;
            json[nameof(Reason)] = Reason;
            json[nameof(Details)] = Details?.ToJson();

            return json;
        }

        public static PerformanceHint Create(string database, string title, string msg, PerformanceHintReason reason, NotificationSeverity notificationSeverity, string source, INotificationDetails details = null)
        {
            return new PerformanceHint(database)
            {
                IsPersistent = true,
                Title = title,
                Message = msg,
                Reason = reason,
                Severity = notificationSeverity,
                Source = source,
                Details = details
            };
        }

        public static string GetKey(PerformanceHintReason reason, string source)
        {
            return $"{NotificationType.PerformanceHint}/{reason}/{source}";
        }
    }
}
