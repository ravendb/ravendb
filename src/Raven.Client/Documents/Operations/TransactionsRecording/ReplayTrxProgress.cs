using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TransactionsRecording
{
    public class ReplayTrxProgress : IOperationProgress
    {
        public long ProcessedCommand { get; set; }

        public float ProcessedPercentage { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(ProcessedCommand)] = ProcessedCommand,
                [nameof(ProcessedPercentage)] = ProcessedPercentage
            };
        }
    }
}
