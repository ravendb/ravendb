using Raven.Client.Documents.Changes;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    public class OperationStatusChange : DatabaseChange
    {
        public long OperationId { get; set; }

        public OperationState State { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                ["OperationId"] = OperationId,
                ["State"] = State.ToJson()
            };
        }
    }
}