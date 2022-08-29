using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class ReplicationHandlerProcessorForGetPerformance : AbstractReplicationHandlerProcessorForGetPerformance<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetPerformance([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected IEnumerable<IAbstractIncomingReplicationHandler> GetIncomingHandlers(DocumentsOperationContext context)
        {
            return RequestHandler.Database.ReplicationLoader.IncomingHandlers;
        }

        protected IEnumerable<IReportOutgoingReplicationPerformance> GetOutgoingReplicationReportsPerformance(DocumentsOperationContext context)
        {
            var reporters = RequestHandler.Database.ReplicationLoader.OutgoingHandlers.Concat<IReportOutgoingReplicationPerformance>(RequestHandler.Database.ReplicationLoader
                .OutgoingConnectionsLastFailureToConnect.Values);
            return reporters;
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var incomingHandlers = GetIncomingHandlers(context);
                var outgoingReplicationReports = GetOutgoingReplicationReportsPerformance(context);
                await WriteResultsAsync(context, incomingHandlers, outgoingReplicationReports);
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<ReplicationPerformance> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
