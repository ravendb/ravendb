using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public class PerformanceHint : Notification
    {
        private PerformanceHint() : base(NotificationType.PerformanceHint)
        {
        }

        public PerformanceHintType HintType { get; private set; }

        public string Source { get; private set; }

        public override string Id => $"{Type}/{HintType}/{Source}";

        public INotificationDetails Details { get; protected set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(Source)] = Source;
            json[nameof(HintType)] = HintType;
            json[nameof(Details)] = Details?.ToJson();

            return json;
        }

        public static PerformanceHint Create(string title, string msg, PerformanceHintType type, NotificationSeverity notificationSeverity, string source, INotificationDetails details = null)
        {
            return new PerformanceHint
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
    }
}