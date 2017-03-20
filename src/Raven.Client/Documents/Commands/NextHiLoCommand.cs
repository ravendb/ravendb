using System;
using System.Net.Http;
using Raven.Client.Documents.Identity;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class NextHiLoCommand : RavenCommand<HiLoResult>
    {
        public string Tag;
        public long LastBatchSize;
        public DateTime LastRangeAt;
        public string IdentityPartsSeparator;
        public long LastRangeMax;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {            
            var path =$"hilo/next?tag={Tag}&lastBatchSize={LastBatchSize}&lastRangeAt={LastRangeAt:o}&identityPartsSeparator={IdentityPartsSeparator}&lastMax={LastRangeMax}"; 

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
            
            url = $"{node.Url}/databases/{node.Database}/" + path;
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.HiLoResult(response);
        }

        public override bool IsReadRequest => true;
    }
}
