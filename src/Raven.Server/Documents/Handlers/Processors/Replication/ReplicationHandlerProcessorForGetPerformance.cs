using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class ReplicationHandlerProcessorForGetPerformance : AbstractReplicationHandlerProcessorForGetPerformance<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetPerformance([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IEnumerable<IAbstractIncomingReplicationHandler> GetIncomingHandlers(DocumentsOperationContext context)
        {
            return RequestHandler.Database.ReplicationLoader.IncomingHandlers;
        }

        protected override IEnumerable<IReportOutgoingReplicationPerformance> GetOutgoingReplicationReportsPerformance(DocumentsOperationContext context)
        {
            var reporters = RequestHandler.Database.ReplicationLoader.OutgoingHandlers.Concat<IReportOutgoingReplicationPerformance>(RequestHandler.Database.ReplicationLoader
                .OutgoingConnectionsLastFailureToConnect.Values);
            return reporters;
        }
    }
}
