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
    public class PatchByIndexCommand : RavenCommand<OperationIdResult>
    {
        public BlittableJsonReaderObject Script;
        public JsonOperationContext Context;
        public string IndexName;
        public IndexQuery QueryToUpdate;
        public QueryOperationOptions Options;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var notNullOptions = Options ?? new QueryOperationOptions();
            var u = $"{node.Url}/databases/{node.Database}";
            url = $"{QueryToUpdate.GetIndexQueryUrl(u, IndexName, "queries")}&allowStale=" +
                  $"{notNullOptions.AllowStale}&maxOpsPerSec={notNullOptions.MaxOpsPerSecond}&details={notNullOptions.RetrieveDetails}";
            
            if (notNullOptions.StaleTimeout != null)
                url += "&staleTimeout=" + notNullOptions.StaleTimeout;

            var request = new HttpRequestMessage
            {
                Method = HttpMethods.Patch,
                Content = new BlittableJsonContent(stream =>
                {
                    Context.Write(stream, Script);
                })
            };

            IsReadRequest = false;
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
                throw new InvalidOperationException("Got null response from the server after doing a patch by index, something is very wrong. ");
            Result = JsonDeserializationClient.OperationIdResult(response);
        }
    }
}