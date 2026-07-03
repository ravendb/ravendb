using Raven.Server.Documents.TasksErrors;
using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public sealed class EtlTaskHealthChangeDetails : INotificationDetails
    {
        public OngoingTaskHealthStatus HealthStatus { get; set; }

        public OngoingTaskHealthStatus? PreviousHealthStatus { get; set; }

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
