using System;
using System.Net.Http;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class DeleteByCollectionCommand : RavenCommand<OperationIdResult>
    {
        public string CollectionName;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/collections/docs?name={CollectionName}";
            IsReadRequest = false;

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
                throw new InvalidOperationException("Got null response from the server after doing a delete by index, something is very wrong. ");
            Result = JsonDeserializationClient.OperationIdResult(response);
        }
    }
}