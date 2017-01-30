using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public interface INotificationDetails
    {
        DynamicJsonValue ToJson();
    }
}