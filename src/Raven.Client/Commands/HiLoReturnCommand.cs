using System.Net.Http;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Commands
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
