using System.Collections.Generic;
using Raven.Client.Data;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents.Data
{
    public class ImportResult : IOperationResult
    {
        public long DocumentsCount;
        public long RevisionDocumentsCount;
        public long IndexesCount;
        public long TransformersCount;
        public long IdentitiesCount;

        public readonly List<string> Warnings = new List<string>();
        public string Message { get; set; }
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(DocumentsCount)] = DocumentsCount,
                [nameof(RevisionDocumentsCount)] = RevisionDocumentsCount,
                [nameof(IndexesCount)] = IndexesCount,
                [nameof(TransformersCount)] = TransformersCount,
                [nameof(IdentitiesCount)] = IdentitiesCount,
                [nameof(Warnings)] = Warnings,
                [nameof(Message)] = Message
            };
        }
    }
}