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

        public override HttpRequestMessage CreateRequest()
        {
            return new HttpRequestMessage(HttpMethod.Get, $"{Url}/databases/{Database}/docs?id={UrlEncode(Id)}");
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            Result = response;
        }
    }
}