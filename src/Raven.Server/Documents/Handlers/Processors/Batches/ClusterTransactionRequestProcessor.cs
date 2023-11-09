using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using static Raven.Server.Utils.MetricCacher.Keys;

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
        public override AsyncWaiter<long?>.RemoveTask CreateClusterTransactionTask(string id, long index, out Task<long?> task)
        {
            return RequestHandler.ServerStore.Cluster.ClusterTransactionWaiter.CreateTaskForDatabase(id, index, RequestHandler.Database, out task);
        }

        public override Task<long?> WaitForDatabaseCompletion(Task<long?> onDatabaseCompletionTask)
        {
            return onDatabaseCompletionTask.WithCancellation(RequestHandler.HttpContext.RequestAborted);
        }

        protected override DateTime GetUtcNow()
        {
            return RequestHandler.Database.Time.GetUtcNow();
        }

        protected override (string DatabaseGroupId, string ClusterTransactionId) GetDatabaseGroupIdAndClusterTransactionId(TransactionOperationContext ctx, string id)
        {
            return (RequestHandler.Database.DatabaseGroupId, RequestHandler.Database.ClusterTransactionId);
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
