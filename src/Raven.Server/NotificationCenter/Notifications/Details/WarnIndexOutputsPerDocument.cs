using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class WarnIndexOutputsPerDocument : INotificationDetails
    {
        public long NumberOfExceedingDocuments { get; set; }

        public string SampleDocumentId { get; set; }

        public int MaxProducedOutputsForDocument { get; set; }

        public string Suggestion { get; set; }

        internal DateTime? LastWarnedAt { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(NumberOfExceedingDocuments)] = NumberOfExceedingDocuments,
                [nameof(SampleDocumentId)] = SampleDocumentId,
                [nameof(MaxProducedOutputsForDocument)] = MaxProducedOutputsForDocument,
                [nameof(Suggestion)] = Suggestion
            };
        }
    }
}