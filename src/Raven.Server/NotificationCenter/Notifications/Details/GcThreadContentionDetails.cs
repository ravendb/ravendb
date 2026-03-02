using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public sealed class GcThreadContentionDetails : INotificationDetails
    {
        public int TotalCores { get; set; }

        public int UtilizedCores { get; set; }

        public string Message { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(TotalCores)] = TotalCores,
                [nameof(UtilizedCores)] = UtilizedCores,
                [nameof(Message)] = Message
            };
        }
    }
}
