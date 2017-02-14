using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    public class BulkOperationResult : IOperationResult
    {
        public long Total { get; set; }

        public string Message => $"Processed ${Total} items.";
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Total"] = Total,
                ["Message"] = Message,
            };
        }

        public bool ShouldPersist => false;
    }
}