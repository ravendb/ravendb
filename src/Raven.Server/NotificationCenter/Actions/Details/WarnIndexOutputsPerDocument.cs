using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions.Details
{
    public class WarnIndexOutputsPerDocument : INotificationDetails
    {
        public string Warning { get; set; }
        public long NumberOfExceedingDocuments { get; set; }
        public string SampleDocumentId { get; set; }
        public int MaxProducedOutputsForDocument { get; set; }
        public string Suggestion { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Warning)] = Warning,
                [nameof(NumberOfExceedingDocuments)] = NumberOfExceedingDocuments,
                [nameof(SampleDocumentId)] = SampleDocumentId,
                [nameof(MaxProducedOutputsForDocument)] = MaxProducedOutputsForDocument,
                [nameof(Suggestion)] = Suggestion
            };
        }
    }
}