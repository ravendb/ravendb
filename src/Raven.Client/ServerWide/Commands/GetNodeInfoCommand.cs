using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Commands
{
    public class NodeInfo : IDynamicJson
    {
        public string NodeTag;
        public string TopologyId;
        public string Certificate;
        public string ClusterStatus;
        public int NumberOfCores;
        public double InstalledMemoryInGb;
        public double UsableMemoryInGb;
        public BuildNumber BuildInfo;
        public OsInfo OsInfo;
        public Guid ServerId;
        public RachisState CurrentState;
        public bool HasFixedPort;
        public int ServerSchemaVersion;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(TopologyId)] = TopologyId,
                [nameof(Certificate)] = Certificate,
                [nameof(ClusterStatus)] = ClusterStatus,
                [nameof(NumberOfCores)] = NumberOfCores,
                [nameof(InstalledMemoryInGb)] = InstalledMemoryInGb,
                [nameof(UsableMemoryInGb)] = UsableMemoryInGb,
                [nameof(BuildInfo)] = BuildInfo,
                [nameof(OsInfo)] = OsInfo,
                [nameof(ServerId)] = ServerId.ToString(),
                [nameof(CurrentState)] = CurrentState,
                [nameof(HasFixedPort)] = HasFixedPort,
                [nameof(ServerSchemaVersion)] = ServerSchemaVersion,
            };
        }
    }

    public class GetNodeInfoCommand : RavenCommand<NodeInfo>
    {
        public GetNodeInfoCommand() { }

        public GetNodeInfoCommand(TimeSpan timeout)
        {
            Timeout = timeout;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/cluster/node-info";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationClient.NodeInfo(response);
        }

        public override bool IsReadRequest => true;
    }
}
