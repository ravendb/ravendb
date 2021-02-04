using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Revisions
{
    public abstract class OperationResult : IOperationResult, IOperationProgress
    {
        public int ScannedRevisions { get; set; }
        public int ScannedDocuments { get; set; }
        public Dictionary<string, string> Warnings { get; set; } = new Dictionary<string, string>();
        public string Message { get; }

        public void Warn(string id, string message)
        {
            Warnings[id] = message;
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Message)] = Message,
                [nameof(ScannedRevisions)] = ScannedRevisions,
                [nameof(ScannedDocuments)] = ScannedDocuments,
                [nameof(Warnings)] = DynamicJsonValue.Convert(Warnings)
            };
        }

        public bool ShouldPersist => false;
    }

    public class EnforceConfigurationResult : OperationResult
    {
        public int RemovedRevisions { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(RemovedRevisions)] = RemovedRevisions;
            return json;
        }
    }

    public class RevertResult : OperationResult
    {
        public int RevertedDocuments { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(RevertedDocuments)] = RevertedDocuments;
            return json;
        }
    }
}
