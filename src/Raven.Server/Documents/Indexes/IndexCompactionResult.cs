using Raven.Client.Data;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes
{
    public class IndexCompactionResult : IOperationResult
    {
        public static IndexCompactionResult Instance = new IndexCompactionResult();

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
    }
}