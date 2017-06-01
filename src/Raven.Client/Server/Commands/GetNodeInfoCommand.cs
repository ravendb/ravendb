using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Commands
{
    public class NodeInfo
    {
        public string NodeTag;
        public string TopologyId;
    }

    class GetNodeInfoCommand : RavenCommand<NodeInfo>
    {
        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/cluster/node-info";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationClient.NodeInfo(response);
        }

        public override bool IsReadRequest => true;
    }
}
