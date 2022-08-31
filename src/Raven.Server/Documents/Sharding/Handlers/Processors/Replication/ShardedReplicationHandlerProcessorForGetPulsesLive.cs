using System;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedReplicationHandlerProcessorForGetPulsesLive : AbstractReplicationHandlerProcessorForGetPulsesLive<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedReplicationHandlerProcessorForGetPulsesLive([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        protected override bool SupportsCurrentNode => false;

        protected override string GetDatabaseName()
        {
            var shardNumber = GetShardNumber();

            return ShardHelper.ToShardName(RequestHandler.DatabaseName, shardNumber);
        }

        protected override ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "fetching data from the orchestrator");
            throw new NotImplementedException();
        }
    }
}
