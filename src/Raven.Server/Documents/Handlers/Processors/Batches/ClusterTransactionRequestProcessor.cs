using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.CodeAnalysis;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Revisions;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Voron;
using static Raven.Server.Documents.TransactionMerger.Commands.JsonPatchCommand;
using static Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged;
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

        public override async Task WaitForDatabaseCompletion(Task<HashSet<string>> onDatabaseCompletionTask, long index, ClusterTransactionOptions options, CancellationToken token)
        {
            var database = RequestHandler.Database;
            var lastCompleted = database.ClusterWideTransactionIndexWaiter.LastIndex;
            HashSet<string> modifiedCollections = null;
            if (lastCompleted < index)
                modifiedCollections = await onDatabaseCompletionTask; // already registered to the token

            if (options.WaitForIndexesTimeout.HasValue)
            {
                long lastDocumentEtag, lastTombstoneEtag;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    lastDocumentEtag = DocumentsStorage.ReadLastDocumentEtag(tx.InnerTransaction);
                    lastTombstoneEtag = DocumentsStorage.ReadLastTombstoneEtag(tx.InnerTransaction);
                    modifiedCollections ??= database.DocumentsStorage.GetCollections(context).Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
                }

                await BatchHandlerProcessorForBulkDocs.WaitForIndexesAsync(database, options.WaitForIndexesTimeout.Value,
                    options.SpecifiedIndexesQueryString, options.WaitForIndexThrow,
                    lastDocumentEtag, lastTombstoneEtag, modifiedCollections, token);
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
