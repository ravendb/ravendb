using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class DatabaseCompactionResult : IOperationResult
    {
        public static DatabaseCompactionResult Instance = new DatabaseCompactionResult();

        private DatabaseCompactionResult()
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