using System;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.ServerWide.Commands;

namespace Raven.Server.Documents.Handlers.Processors.Batches
{
    public class ClusterTransactionRequestProcessor : AbstractClusterTransactionRequestProcessor<DatabaseRequestHandler, MergedBatchCommand>
    {
        private readonly DatabaseTopology _topology;

        public ClusterTransactionRequestProcessor(DatabaseRequestHandler requestHandler, [NotNull] DatabaseTopology topology)
            : base(requestHandler)
        {
            _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        }

        protected override ArraySegment<BatchRequestParser.CommandData> GetParsedCommands(MergedBatchCommand command) => command.ParsedCommands;

        protected override ClusterTransactionCommand CreateClusterTransactionCommand(
            ArraySegment<BatchRequestParser.CommandData> parsedCommands,
            ClusterTransactionCommand.ClusterTransactionOptions options,
            string raftRequestId)
        {
            return new ClusterTransactionCommand(
                RequestHandler.Database.Name,
                RequestHandler.Database.IdentityPartsSeparator,
                _topology,
                parsedCommands,
                options,
                raftRequestId);
        }
    }
}
