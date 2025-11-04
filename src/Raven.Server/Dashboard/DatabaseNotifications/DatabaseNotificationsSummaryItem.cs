using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.DatabaseNotifications;

public sealed class DatabaseNotificationsSummaryItem : IDynamicJson
{
    public string DatabaseName { get; set; }
    public NotificationCounts NotificationCounts { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DatabaseName)] = DatabaseName,
            [nameof(NotificationCounts)] = NotificationCounts
        };
    }
}
