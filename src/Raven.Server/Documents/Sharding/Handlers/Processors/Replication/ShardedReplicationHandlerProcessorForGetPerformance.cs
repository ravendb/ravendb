using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedReplicationHandlerProcessorForGetPerformance : AbstractReplicationHandlerProcessorForGetPerformance<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedReplicationHandlerProcessorForGetPerformance([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IEnumerable<IAbstractIncomingReplicationHandler> GetIncomingHandlers(TransactionOperationContext context)
        {
            return RequestHandler.DatabaseContext.Replication.IncomingHandlers;
        }

        protected override IEnumerable<IReportOutgoingReplicationPerformance> GetOutgoingReplicationReportsPerformance(TransactionOperationContext context)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "handle outgoing performance");
            return null;
        }
    }
}
