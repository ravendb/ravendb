using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public class PerformanceHint : Notification
    {
        private PerformanceHint(string database) : base(NotificationType.PerformanceHint, database)
        {
        }

        public PerformanceHintType HintType { get; private set; }

        public string Source { get; private set; }

        public override string Id => GetKey(HintType, Source);

        public INotificationDetails Details { get; protected set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(Source)] = Source;
            json[nameof(HintType)] = HintType;
            json[nameof(Details)] = Details?.ToJson();

            return json;
        }

        public static PerformanceHint Create(string database, string title, string msg, PerformanceHintType type, NotificationSeverity notificationSeverity, string source, INotificationDetails details = null)
        {
            return new PerformanceHint(database)
            {
                IsPersistent = true,
                Title = title,
                Message = msg,
                HintType = type,
                Severity = notificationSeverity,
                Source = source,
                Details = details
            };
        }

        public static string GetKey(PerformanceHintType type, string source)
        {
            return $"{NotificationType.PerformanceHint}/{type}/{source}";
        }
    }
}
