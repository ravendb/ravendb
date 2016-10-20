using Raven.Client.Data;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents.Data
{
    public class ExportResult: IOperationResult
    {
        public long LastDocsEtag;
        public long LastRevisionDocumentsEtag;

        public string Message { get; }
        public DynamicJsonValue ToJson()
        {
            throw new System.NotImplementedException();
        }
    }
}