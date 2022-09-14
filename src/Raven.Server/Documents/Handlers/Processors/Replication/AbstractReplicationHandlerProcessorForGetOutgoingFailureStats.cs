using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Documents.Replication;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Replication;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetOutgoingFailureStats<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<ReplicationOutgoingsFailurePreview, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetOutgoingFailureStats([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override RavenCommand<ReplicationOutgoingsFailurePreview> CreateCommandForNode(string nodeTag)
        {
            return new GetReplicationOutgoingsFailureInfoCommand(nodeTag);
        }
    }

    public class ReplicationOutgoingsFailurePreview
    {
        public IDictionary<ReplicationNode, ConnectionShutdownInfo> Stats;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Stats)] = new DynamicJsonArray(Stats.Select(OutgoingFailureInfoToJson))
            };
        }

        private DynamicJsonValue OutgoingFailureInfoToJson(KeyValuePair<ReplicationNode, ConnectionShutdownInfo> kvp)
        {
            return new DynamicJsonValue
            {
                ["Key"] = ReplicationNodeToJson(kvp.Key),
                ["Value"] = kvp.Value
            };
        }
    
        private DynamicJsonValue ReplicationNodeToJson(ReplicationNode replicationNode)
        {
            var json = replicationNode.ToJson();
            json["Type"] = replicationNode.GetType().ToString();
            return json;
        }
    }
}
