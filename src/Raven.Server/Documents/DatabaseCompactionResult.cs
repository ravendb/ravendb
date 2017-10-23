using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public sealed class DatabaseCompactionResult : IOperationResult
    {
        public static readonly DatabaseCompactionResult Instance = new DatabaseCompactionResult();

        private DatabaseCompactionResult()
        {
        }

        public long SizeBeforeCompactionInMb;
        public long SizeAfterCompactionInMb;

        public string Message => $"Compaction finished. Reduced storage size from {SizeBeforeCompactionInMb} MB to {SizeAfterCompactionInMb} MB (-{(int)((double)(SizeBeforeCompactionInMb - SizeAfterCompactionInMb) / SizeBeforeCompactionInMb * 100)}%).";

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Message"] = Message
            };
        }

        public bool ShouldPersist => true;
    }
}
