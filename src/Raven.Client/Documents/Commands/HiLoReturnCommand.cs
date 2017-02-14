using System.Net.Http;
using Raven.Client.Documents.Identity;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class HiLoReturnCommand : RavenCommand<HiLoResult>
    {
        public string Tag;
        public long Last;
        public long End;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var path = $"hilo/return?tag={Tag}&end={End}&last={Last}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put
            };

            url = $"{node.Url}/databases/{node.Database}/" + path;
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache) { }
        public override bool IsReadRequest => false;
    }
}
