using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Commands
{
    public class GetClusterTopologyCommand : RavenCommand<ClusterTopologyResponse>
    {
        public GetClusterTopologyCommand()
        {
            FailedNodes = new Dictionary<ServerNode, ExceptionDispatcher.ExceptionSchema>();
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/cluster/topology?url={node.Url}";
           
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationClient.ClusterTopology(response);
        }

        public override bool IsReadRequest => true;
    }
}