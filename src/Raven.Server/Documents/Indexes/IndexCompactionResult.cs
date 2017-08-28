using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes
{
    public sealed class IndexCompactionResult : IOperationResult
    {
        public static readonly IndexCompactionResult Instance = new IndexCompactionResult();

        private IndexCompactionResult()
        {
        }

        public string Message => "Compaction finished.";

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Message"] = Message
            };
        }

        public bool ShouldPersist => false;
    }
}
