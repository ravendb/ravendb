using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
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

        public override async Task WaitForDatabaseCompletion(Task onDatabaseCompletionTask, long index, ClusterTransactionOptions options, ArraySegment<BatchRequestParser.CommandData> parsedCommands, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            var database = RequestHandler.Database;
            var lastCompleted = Interlocked.Read(ref database.LastCompletedClusterTransactionIndex);
            if (lastCompleted < index)
                await onDatabaseCompletionTask; // already registered to the token

            if (options.WaitForIndexesTimeout.HasValue)
            {
                long lastDocumentEtag, lastTombstoneEtag;
                HashSet<string> modifiedCollections;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    lastDocumentEtag = DocumentsStorage.ReadLastDocumentEtag(tx.InnerTransaction);
                    lastTombstoneEtag = DocumentsStorage.ReadLastTombstoneEtag(tx.InnerTransaction);
                    modifiedCollections = GetModifiedCollections(context, database, parsedCommands);
                }

                await BatchHandlerProcessorForBulkDocs.WaitForIndexesAsync(database, options.WaitForIndexesTimeout.Value,
                    options.SpecifiedIndexesQueryString, options.WaitForIndexThrow,
                    lastDocumentEtag, lastTombstoneEtag, modifiedCollections, token);
            }
        }

        private static HashSet<string> GetModifiedCollections(DocumentsOperationContext context, DocumentDatabase database, ArraySegment<BatchRequestParser.CommandData> parsedCommands)
        {
            var modifiedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (BatchRequestParser.CommandData cmd in parsedCommands)
            {
                string collectionName;

                switch (cmd.Type)
                {
                    case CommandType.PUT:
                        collectionName = CollectionName.GetCollectionName(cmd.Document);
                        break;
                    case CommandType.DELETE:
                        using (DocumentIdWorker.GetSliceFromId(context, cmd.Id, out Slice lowerId))
                        {
                            var local = database.DocumentsStorage.GetDocumentOrTombstone(context, lowerId, throwOnConflict: false);
                            if (local.Tombstone == null)
                                continue;
                            collectionName = local.Tombstone.Collection.ToString();
                        }

                        break;
                    case CommandType.CompareExchangePUT:
                    case CommandType.CompareExchangeDELETE:
                        continue;
                    default:
                        throw new InvalidOperationException(
                            $"Database cluster transaction command type can be {CommandType.PUT} or {CommandType.DELETE} but got {cmd.Type}");
                }
                modifiedCollections.Add(collectionName);
            }

            return modifiedCollections;
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
