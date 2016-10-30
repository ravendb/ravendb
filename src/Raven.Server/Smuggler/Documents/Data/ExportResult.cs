using Raven.Client.Data;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents.Data
{
    public class ExportResult: IOperationResult
    {
        public long LastDocsEtag;
        public long LastRevisionDocumentsEtag;
        public int DocumentCount { get; set; }

        public string Message => $"Exported {DocumentCount} documents.";
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(DocumentCount)] = DocumentCount,
                ["Message"] = Message
            };
        }
    }
}