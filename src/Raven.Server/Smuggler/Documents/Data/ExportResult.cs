using Raven.Client.Data;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents.Data
{
    public class ExportResult: IOperationResult
    {
        public long LastDocsEtag { get; set; }
        public long LastRevisionDocumentsEtag { get; set; }
        public long ExportedDocuments { get; set; }

        public string Message { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(LastDocsEtag)] = LastDocsEtag,
                [nameof(LastRevisionDocumentsEtag)] = LastRevisionDocumentsEtag,
                [nameof(ExportedDocuments)] = ExportedDocuments,
                [nameof(Message)] = Message
            };
        }
    }
}