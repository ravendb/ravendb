using System;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers.Processors.Batches
{
    public class ClusterTransactionRequestProcessor : AbstractClusterTransactionRequestProcessor<BatchHandler.MergedBatchCommand>
    {
        private readonly DatabaseTopology _topology;

        public ClusterTransactionRequestProcessor(RequestHandler handler, string database, char identitySeparator, DatabaseTopology topology) 
            : base(handler, database, identitySeparator)
        {
            _topology = topology;
        }

        protected override ClusterTransactionCommand CreateClusterTransactionCommand(
            ArraySegment<BatchRequestParser.CommandData> parsedCommands,
            ClusterTransactionCommand.ClusterTransactionOptions options,
            string raftRequestId)
        {
            return new ClusterTransactionCommand(
                Database,
                IdentitySeparator,
                _topology,
                parsedCommands,
                options,
                raftRequestId);
        }
    }
}
