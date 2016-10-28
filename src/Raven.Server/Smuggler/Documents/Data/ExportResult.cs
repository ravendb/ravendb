using Raven.Client.Data;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents.Data
{
    public class ExportResult: IOperationResult
    {
        public long LastDocsEtag;
        public long LastRevisionDocumentsEtag;
        public int DocumentExported { get; set; }

        public string Message => $"Exported {DocumentExported} documents.";
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["documentExported"] = DocumentExported,
                ["Message"] = Message
            };
        }
    }
}