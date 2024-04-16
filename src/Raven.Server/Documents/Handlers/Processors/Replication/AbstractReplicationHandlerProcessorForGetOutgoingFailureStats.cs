using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
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

    public sealed class ReplicationOutgoingsFailurePreview : IFillFromBlittableJson
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
                ["Key"] = kvp.Key.ToJson(),
                ["Value"] = kvp.Value.ToJson()
            };
        }

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            Stats = new Dictionary<ReplicationNode, ConnectionShutdownInfo>();
            if (json.TryGetMember(nameof(Stats), out var result) && result is BlittableJsonReaderArray bjra)
            {
                foreach (BlittableJsonReaderObject bjro in bjra)
                {
                    if (bjro.TryGet("Key", out BlittableJsonReaderObject keyBjro) &&
                        bjro.TryGet("Value", out BlittableJsonReaderObject valueBjro))
                    {
                        var key = ReplicationHelper.GetReplicationNodeByType(keyBjro);
                        var value = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<ConnectionShutdownInfo>(valueBjro, "ConnectionShutdownInfo");

                        Stats.Add(key, value);
                    }
                }
            }
        }
    }
}
