using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Replication.Stats;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetIncomingActivityTimes<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<ReplicationIncomingLastActivityTimePreview, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetIncomingActivityTimes([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override RavenCommand<ReplicationIncomingLastActivityTimePreview> CreateCommandForNode(string nodeTag)
        {
            return new GetReplicationIncomingActivityTimesInfoCommand(nodeTag);
        }
    }

    public class ReplicationIncomingLastActivityTimePreview
    {
        public IDictionary<IncomingConnectionInfo, DateTime> IncomingActivityTimes;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                ["Stats"] = new DynamicJsonArray(IncomingActivityTimes.Select(IncomingActivityTimeToJson))
            };
        }

        private DynamicJsonValue IncomingActivityTimeToJson(KeyValuePair<IncomingConnectionInfo, DateTime> kvp)
        {
            return new DynamicJsonValue
            {
                ["Key"] = kvp.Key.ToJson(),
                ["Value"] = kvp.Value
            };
        }
    }
}
