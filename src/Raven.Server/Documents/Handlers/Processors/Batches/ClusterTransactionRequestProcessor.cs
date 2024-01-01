using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;

namespace Raven.Server.Documents.Handlers.Processors.Batches
{
    public sealed class ClusterTransactionRequestProcessor : AbstractClusterTransactionRequestProcessor<DatabaseRequestHandler, MergedBatchCommand>
    {
        private readonly DatabaseTopology _topology;

        public ClusterTransactionRequestProcessor(DatabaseRequestHandler requestHandler, [NotNull] DatabaseTopology topology)
            : base(requestHandler)
        {
            _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        }

        protected override ArraySegment<BatchRequestParser.CommandData> GetParsedCommands(MergedBatchCommand command) => command.ParsedCommands;

        protected override ClusterConfiguration GetClusterConfiguration() => RequestHandler.Database.Configuration.Cluster;

        public override Task<Task> WaitForDatabaseCompletion(Task<Task> onDatabaseCompletionTask, long index, CancellationToken token)
        {
            // check index
            var lastCompleted = Interlocked.Read(ref RequestHandler.Database.LastCompletedClusterTransactionIndex);
            if (lastCompleted >= index)
            {
                return Task.FromResult(Task.CompletedTask);
            }

            return onDatabaseCompletionTask.WithCancellation(token);
        }

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
