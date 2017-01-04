using System;
using System.Net.Http;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class DeleteByIndexCommand : RavenCommand<OperationIdResult>
    {
        public string IndexName;
        public IndexQuery QueryToDelete;
        public QueryOperationOptions Options;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var notNullOptions = Options ?? new QueryOperationOptions();
            var u = $"{node.Url}/databases/{node.Database}";
            url = $"{QueryToDelete.GetIndexQueryUrl(u, IndexName, "queries")}&allowStale=" +
                  $"{notNullOptions.AllowStale}&details={notNullOptions.RetrieveDetails}";

            if (notNullOptions.MaxOpsPerSecond != null)
                url += "&maxOpsPerSec=" + notNullOptions.StaleTimeout;
            if (notNullOptions.StaleTimeout != null)
                url += "&staleTimeout=" + notNullOptions.StaleTimeout;

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