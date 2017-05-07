using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

public class WarningDetails : INotificationDetails
{
    public WarningDetails(string e)
    {
        WarningMessage = e;
    }

    public string WarningMessage { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue(GetType())
        {
            [nameof(WarningMessage)] = WarningMessage
        };
    }
}