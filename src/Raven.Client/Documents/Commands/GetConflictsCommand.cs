using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    //not sure if this should be done for multiple doc IDs
    //for now it will work for single conflicted document id
    public class GetConflictsCommand : RavenCommand<GetConflictsResult>
    {
        private readonly string _id;
        public override bool IsReadRequest => true;

        public GetConflictsCommand(string id)
        {
            _id = id;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replication/conflicts?docId={_id}";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.GetConflictsResult(response);
        }
    }
}
