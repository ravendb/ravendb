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
    internal abstract class AbstractReplicationHandlerProcessorForGetOutgoingReconnectionQueue<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<ReplicationOutgoingReconnectionQueuePreview, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetOutgoingReconnectionQueue([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override RavenCommand<ReplicationOutgoingReconnectionQueuePreview> CreateCommandForNode(string nodeTag)
        {
            return new GetReplicationOutgoingReconnectionQueueCommand(nodeTag);
        }
    }

    public sealed class ReplicationOutgoingReconnectionQueuePreview : IFillFromBlittableJson
    {
        public List<ReplicationNode> QueueInfo;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                ["Queue-Info"] = new DynamicJsonArray(QueueInfo.Select(o => o.ToJson()))
            };
        }

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            QueueInfo = new List<ReplicationNode>();
            if (json.TryGetMember("Queue-Info", out var result) && result is BlittableJsonReaderArray bjra)
            {
                foreach (BlittableJsonReaderObject bjro in bjra)
                {
                    var replicationNode = ReplicationHelper.GetReplicationNodeByType(bjro);
                    QueueInfo.Add(replicationNode);
                }
            }
        }
    }
}
