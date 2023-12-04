using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using static Raven.Server.ServerWide.Commands.ClusterTransactionCommand;
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

        public override Task<long?> WaitForDatabaseCompletion(Task<long?> onDatabaseCompletionTask, CancellationToken token)
        {
            return onDatabaseCompletionTask.WithCancellation(token);
        }

        protected override DateTime GetUtcNow()
        {
            return RequestHandler.Database.Time.GetUtcNow();
        }

        protected override void GenerateDatabaseCommandsEvaluatedResults(List<ClusterTransactionDataCommand> databaseCommands,
            long index, long count, DateTime lastModified, bool? disableAtomicDocumentWrites,
            DynamicJsonArray commandsResults)
        {
            if (count < 0)
                throw new InvalidOperationException($"ClusterTransactionCommand result is invalid - count lower then 0 ({count}).");

            foreach (var dataCmd in databaseCommands)
            {
                count++;
                var cv = GenerateChangeVector(index, count, disableAtomicDocumentWrites,
                    RequestHandler.Database.DatabaseGroupId, RequestHandler.Database.ClusterTransactionId);

                commandsResults.Add(GetCommandResultJson(dataCmd, cv, lastModified));
            }
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
