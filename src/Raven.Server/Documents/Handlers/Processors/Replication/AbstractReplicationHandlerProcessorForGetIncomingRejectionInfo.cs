using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Stats;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetIncomingRejectionInfo<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<ReplicationIncomingRejectionInfoPreview, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetIncomingRejectionInfo([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override RavenCommand<ReplicationIncomingRejectionInfoPreview> CreateCommandForNode(string nodeTag)
        {
            return new GetIncomingReplicationRejectionInfoCommand(nodeTag);
        }
    }

    public class ReplicationIncomingRejectionInfoPreview
    {
        public IDictionary<IncomingConnectionInfo, ConcurrentQueue<ReplicationLoader.IncomingConnectionRejectionInfo>> Stats;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Stats)] = new DynamicJsonArray(Stats.Select(IncomingRejectionInfoToJson))
            };
        }

        private DynamicJsonValue IncomingRejectionInfoToJson(KeyValuePair<IncomingConnectionInfo, ConcurrentQueue<ReplicationLoader.IncomingConnectionRejectionInfo>> kvp)
        {
            return new DynamicJsonValue
            {
                ["Key"] = kvp.Key.ToJson(),
                ["Value"] = new DynamicJsonArray(kvp.Value.Select(x => new DynamicJsonValue
                {
                    ["Reason"] = x.Reason,
                    ["When"] = x.When
                }))
            };
        }
    }
}
