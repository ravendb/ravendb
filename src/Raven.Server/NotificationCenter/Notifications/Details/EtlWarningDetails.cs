using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public sealed class EtlWarningDetails : INotificationDetails
    {
        public string DocumentId { get; set; }
        public string TimeSeriesName { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(TimeSeriesName)] = TimeSeriesName
            };
        }
    }
}
