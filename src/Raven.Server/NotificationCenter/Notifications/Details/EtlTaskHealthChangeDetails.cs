using System;
using Raven.Server.Documents.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public sealed class EtlTaskHealthChangeDetails : INotificationDetails
    {
        public EtlProcessHealthStatus HealthStatus { get; set; }

        public EtlProcessHealthStatus? PreviousHealthStatus { get; set; }

        public DateTime? PreviousHealthStatusChangeAt { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(HealthStatus)] = HealthStatus,
                [nameof(PreviousHealthStatus)] = PreviousHealthStatus,
                [nameof(PreviousHealthStatusChangeAt)] = PreviousHealthStatusChangeAt
            };
        }
    }
}
