using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class TopologyHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/topology", "GET")]
        public Task GetTopology()
        {
            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, GenerateTopology(context));
            }
            return Task.CompletedTask;
        }
        
        private DynamicJsonValue GenerateTopology(DocumentsOperationContext context)
        {
            Document replicationConfigDocument;
            using (context.OpenReadTransaction())
            {
                replicationConfigDocument = Database.DocumentsStorage.Get(context, Constants.Replication.DocumentReplicationConfiguration);
            }

            //This is the case where we don't have real replication topology.
            if (replicationConfigDocument == null)
            {
                return GetEmptyTopology();
            }

            var replicationDocument = JsonDeserializationServer.ReplicationDocument(replicationConfigDocument.Data);
            if (replicationDocument.Destinations.Count == 0)
                return GetEmptyTopology();

            throw new NotImplementedException();
        }

        private DynamicJsonValue GetEmptyTopology()
        {
            return new DynamicJsonValue
            {
                [nameof(Topology.LeaderNode)] = new DynamicJsonValue
                {
                    [nameof(ServerNode.Url)] =
                        GetStringQueryString("url", required: false) ?? Server.Configuration.Core.ServerUrl,
                    [nameof(ServerNode.Database)] = Database.Name,
                },
                [nameof(Topology.Nodes)] = null,
                [nameof(Topology.ReadBehavior)] = ReadBehavior.LeaderWithFailoverWhenRequestTimeSlaThresholdIsReached.ToString(),
                [nameof(Topology.WriteBehavior)] = WriteBehavior.LeaderOnly.ToString(),
                [nameof(Topology.SLA)] = new DynamicJsonValue
                {
                    [nameof(TopologySla.RequestTimeThresholdInMilliseconds)] = 100,
                },
                [nameof(Topology.Etag)] = -1,
            };
        }

        private IEnumerable<DynamicJsonValue> GenerateNodesFromReplicationDocument(ReplicationDocument replicationDocument)
        {
            foreach (var des in replicationDocument.Destinations)
            {
                if( des.CanBeFailover() == false || des.Disabled || des.IgnoredClient || des.SpecifiedCollections?.Count > 0 )
                    continue;
                yield return new DynamicJsonValue
                {
                    [nameof(ServerNode.Url)] = des.Url,
                    [nameof(ServerNode.ApiKey)] = des.ApiKey,
                    [nameof(ServerNode.Database)] = des.Database
                };
            }
        }
    }
}