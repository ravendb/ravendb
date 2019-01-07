using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class RevertResult : IOperationResult, IOperationProgress
    {
        public int ScannedRevisions { get; set; }
        public int RevertedDocuments { get; set; }
        public int ScannedDocuments { get; set; }
        public Dictionary<string, string> Warnings { get; set; } = new Dictionary<string, string>();
        public string Message { get; }
  
        public void Warn(string id, string message)
        {
            Warnings[id] = message;
        }
  
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Message)] = Message,
                [nameof(ScannedRevisions)] = ScannedRevisions,
                [nameof(ScannedDocuments)] = ScannedDocuments,
                [nameof(RevertedDocuments)] = RevertedDocuments,
                [nameof(Warnings)] = DynamicJsonValue.Convert(Warnings)
            };
        }

        public bool ShouldPersist => false;
    }
}
