using System.Collections.Generic;
using System.Net.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetDocumentCommand : RavenCommand<BlittableJsonReaderObject>
    {
        public string Id;

        public string[] Ids;
        public string[] Includes;

        public string Transformer;
        public Dictionary<string, BlittableJsonReaderBase> TransformerParameters;

        public bool MetadataOnly;

        public GetDocumentCommand(string serverUrl, string database) : base(serverUrl, database)
        {
        }

        public override HttpRequestMessage CreateRequest()
        {
            return new HttpRequestMessage(HttpMethod.Get, $"{ServerUrl}/databases/{Database}/docs?id={UrlEncode(Id)}");
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            Result = response;
        }
    }
}