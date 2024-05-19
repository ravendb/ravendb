using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Microsoft.Net.Http.Headers;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;
using static Raven.Server.ServerWide.Commands.ClusterTransactionCommand;
using Constants = Raven.Client.Constants;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.Handlers
{
    public class BatchHandler : DatabaseRequestHandler
    {
        private static TimeSeriesStorage.AppendOptions AppendOptionsForTimeSeriesCopy = new() { VerifyName = false };

        [RavenAction("/databases/*/bulk_docs", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkDocs()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var token = CreateHttpRequestBoundOperationToken())
            using (var command = new MergedBatchCommand(Database))
            {
                var contentType = HttpContext.Request.ContentType;
                try
                {
                    if (contentType == null ||
                        contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
                    {
                        await BatchRequestParser.BuildCommandsAsync(context, command, RequestBodyStream(), Database, ServerStore, token.Token);
                    }
                    else if (contentType.StartsWith("multipart/mixed", StringComparison.OrdinalIgnoreCase) ||
                             contentType.StartsWith("multipart/form-data", StringComparison.OrdinalIgnoreCase))
                    {
                        await ParseMultipart(context, command, token.Token);
                    }
                    else
                        ThrowNotSupportedType(contentType);
                }
                finally
                {
                    if (TrafficWatchManager.HasRegisteredClients)
                    {
                        BatchTrafficWatch(command.ParsedCommands);
                    }
                }

                var disableAtomicDocumentWrites = GetBoolValueQueryString("disableAtomicDocumentWrites", required: false) ??
                                                  Database.Configuration.Cluster.DisableAtomicDocumentWrites;

                CheckBackwardCompatibility(ref disableAtomicDocumentWrites);

                var waitForIndexesTimeout = GetTimeSpanQueryString("waitForIndexesTimeout", required: false);
                var waitForIndexThrow = GetBoolValueQueryString("waitForIndexThrow", required: false) ?? true;
                var specifiedIndexesQueryString = HttpContext.Request.Query["waitForSpecificIndex"];

                if (command.IsClusterTransaction)
                {
                    await HandleClusterTransaction(context, command, disableAtomicDocumentWrites, specifiedIndexesQueryString, waitForIndexesTimeout, waitForIndexThrow,
                        token.Token);

                    return;
                }

                if (waitForIndexesTimeout != null)
                    command.ModifiedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                try
                {
                    await Database.TxMerger.Enqueue(command);
                }
                catch (ConcurrencyException)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
                    throw;
                }

                var waitForReplicasTimeout = GetTimeSpanQueryString("waitForReplicasTimeout", required: false);
                if (waitForReplicasTimeout != null)
                {
                    var numberOfReplicasStr = GetStringQueryString("numberOfReplicasToWaitFor", required: false) ?? "1";
                    var throwOnTimeoutInWaitForReplicas = GetBoolValueQueryString("throwOnTimeoutInWaitForReplicas", required: false) ?? true;

                    await WaitForReplicationAsync(Database, waitForReplicasTimeout.Value, numberOfReplicasStr, throwOnTimeoutInWaitForReplicas, command.LastChangeVector);
                }

                if (waitForIndexesTimeout != null)
                {
                    long lastEtag = ChangeVectorUtils.GetEtagById(command.LastChangeVector, Database.DbBase64Id);
                    await WaitForIndexesAsync(Database, waitForIndexesTimeout.Value, specifiedIndexesQueryString.ToList(), waitForIndexThrow,
                        lastEtag, command.LastTombstoneEtag, command.ModifiedCollections);
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(BatchCommandResult.Results)] = command.Reply
                    });
                }
            }
        }

        private void CheckBackwardCompatibility(ref bool disableAtomicDocumentWrites)
        {
            if (disableAtomicDocumentWrites)
                return;

            if (RequestRouter.TryGetClientVersion(HttpContext, out var clientVersion) == false)
            {
                disableAtomicDocumentWrites = true;
                return;
            }

            if (clientVersion.Major < 5 || (clientVersion.Major == 5 && clientVersion.Minor < 2))
            {
                disableAtomicDocumentWrites = true;
            }
        }

        private void ValidateCommandForClusterWideTransaction(ArraySegment<BatchRequestParser.CommandData> parsedCommands, DatabaseTopology databaseTopology, bool disableAtomicDocumentWrites)
        {
            if (databaseTopology.Promotables.Contains(ServerStore.NodeTag))
                throw new DatabaseNotRelevantException("Cluster transaction can't be handled by a promotable node.");

            foreach (var commandData in parsedCommands)
            {
                switch (commandData.Type)
                {
                    case CommandType.CompareExchangePUT:
                    case CommandType.CompareExchangeDELETE:

                        if (disableAtomicDocumentWrites == false)
                        {
                            if (ClusterWideTransactionHelper.IsAtomicGuardKey(commandData.Id))
                                throw new CompareExchangeInvalidKeyException($"You cannot manipulate the atomic guard '{commandData.Id}' via the cluster-wide session");
                        }

                        break;
                    case CommandType.PUT:
                    case CommandType.DELETE:
                        if (commandData.Type == CommandType.PUT)
                        {
                            if (commandData.SeenAttachments)
                                throw new NotSupportedException($"The document {commandData.Id} has attachments, this is not supported in cluster wide transaction.");

                            if (commandData.SeenCounters)
                                throw new NotSupportedException($"The document {commandData.Id} has counters, this is not supported in cluster wide transaction.");

                            if (commandData.SeenTimeSeries)
                                throw new NotSupportedException($"The document {commandData.Id} has time series, this is not supported in cluster wide transaction.");
                        }
                        break;

                    default:
                        throw new ArgumentOutOfRangeException($"The command type {commandData.Type} is not supported in cluster transaction.");
                }
            }
        }

        public class ClusterTransactionCompletionResult
        {
            public Task IndexTask;
            public DynamicJsonArray Array;
        }

        private void BatchTrafficWatch(ArraySegment<BatchRequestParser.CommandData> parsedCommands)
        {
            var sb = new StringBuilder();
            for (var i = parsedCommands.Offset; i < (parsedCommands.Offset + parsedCommands.Count); i++)
            {
                // log script and args if type is patch
                if (parsedCommands.Array[i].Type == CommandType.PATCH)
                {
                    sb.Append(parsedCommands.Array[i].Type).Append("    ")
                        .Append(parsedCommands.Array[i].Id).Append("    ")
                        .Append(parsedCommands.Array[i].Patch.Script).Append("    ")
                        .Append(parsedCommands.Array[i].PatchArgs).AppendLine();
                }
                else
                {
                    sb.Append(parsedCommands.Array[i].Type).Append("    ")
                        .Append(parsedCommands.Array[i].Id).AppendLine();
                }
            }
            // add sb to httpContext
            AddStringToHttpContext(sb.ToString(), TrafficWatchChangeType.BulkDocs);
        }

        public async Task HandleClusterTransaction(JsonOperationContext context, MergedBatchCommand command, 
            bool disableAtomicDocumentWrites, StringValues specifiedIndexesQueryString, TimeSpan? waitForIndexesTimeout, bool waitForIndexThrow,
            CancellationToken token)
        {
            var parsedCommands = command.ParsedCommands;
            var topology = ServerStore.LoadDatabaseTopology(Database.Name);

            ValidateCommandForClusterWideTransaction(parsedCommands, topology, disableAtomicDocumentWrites);


            var raftRequestId = GetRaftRequestIdFromQuery();

            var options =
                new ClusterTransactionOptions(taskId: raftRequestId, disableAtomicDocumentWrites,
                    ClusterCommandsVersionManager.CurrentClusterMinimalVersion)
                {
                    WaitForIndexesTimeout = waitForIndexesTimeout,
                    WaitForIndexThrow = waitForIndexThrow,
                    SpecifiedIndexesQueryString = specifiedIndexesQueryString.Count > 0 ? specifiedIndexesQueryString.ToList() : null
                };


            var clusterTransactionCommand =
                new ClusterTransactionCommand(Database.Name, Database.IdentityPartsSeparator, topology, command.ParsedCommands, options, raftRequestId)
                {
                    Timeout = Timeout.InfiniteTimeSpan // we rely on the http token to cancel the command
                };

            DynamicJsonArray array;
            long index;

            using (Database.ClusterTransactionWaiter.CreateTask(id: options.TaskId, out var tcs))
            await using (token.Register(() => tcs.TrySetCanceled()))
            {
                (index, object result) = await ServerStore.SendToLeaderAsync(clusterTransactionCommand, token);
                array = await GetClusterTransactionDatabaseCommandsResults(result, clusterTransactionCommand.DatabaseCommandsCount, index, options, onDatabaseCompletionTask: tcs.Task, token);
            }

            token.ThrowIfCancellationRequested();

            foreach (var clusterCommands in clusterTransactionCommand.ClusterCommands)
            {
                array.Add(new DynamicJsonValue
                {
                    [nameof(ICommandData.Type)] = clusterCommands.Type,
                    [nameof(ICompareExchangeValue.Key)] = clusterCommands.Id,
                    [nameof(ICompareExchangeValue.Index)] = index
                });
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(BatchCommandResult.Results)] = array,
                    [nameof(BatchCommandResult.TransactionIndex)] = index
                });
            }
        }

        private async Task<DynamicJsonArray> GetClusterTransactionDatabaseCommandsResults(object result, long databaseCommandsCount, long index, ClusterTransactionOptions options, Task<HashSet<string>> onDatabaseCompletionTask, CancellationToken token)
        {
            if (result is List<ClusterTransactionErrorInfo> errors)
                ThrowClusterTransactionConcurrencyException(errors);

            if (databaseCommandsCount == 0)
                return new DynamicJsonArray();

            ServerStore.ForTestingPurposes?.AfterCommitInClusterTransaction?.Invoke();

            if (result is ClusterTransactionResult clusterTxResult)
            {
                await WaitForDatabaseCompletion(onDatabaseCompletionTask, index, options, token);
                return clusterTxResult.GeneratedResult;
            }

            // leader isn't updated (thats why the result is empty),
            // so we'll try to take the result from the local history log.
            await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index, Timeout.InfiniteTimeSpan, token);
            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                if (ServerStore.Engine.LogHistory.TryGetResultByGuid<ClusterTransactionResult>(ctx, options.TaskId, out var clusterTxLocalResult))
                    return clusterTxLocalResult.GeneratedResult;
            }

            throw new InvalidOperationException(
                "We are not able to verify that the cluster-wide transaction was succeeded. please consider to upgrade your leader to the latest stable version.");
        }

        private void ThrowClusterTransactionConcurrencyException(List<ClusterTransactionErrorInfo> errors)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
            throw new ClusterTransactionConcurrencyException($"Failed to execute cluster transaction due to the following issues: {string.Join(Environment.NewLine, errors.Select(e => e.Message))}")
            {
                ConcurrencyViolations = errors.Select(e => e.Violation).ToArray()
            };
        }

        private async Task WaitForDatabaseCompletion(Task<HashSet<string>> onDatabaseCompletionTask, long index, ClusterTransactionOptions options, CancellationToken token)
        {
            var lastCompleted = Database.ClusterWideTransactionIndexWaiter.LastIndex;
            HashSet<string> modifiedCollections = null;
            if (lastCompleted < index)
                modifiedCollections = await onDatabaseCompletionTask; // already registered to the token

            if (options.WaitForIndexesTimeout.HasValue)
            {
                long lastDocumentEtag, lastTombstoneEtag;

                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    lastDocumentEtag = DocumentsStorage.ReadLastDocumentEtag(tx.InnerTransaction);
                    lastTombstoneEtag = DocumentsStorage.ReadLastTombstoneEtag(tx.InnerTransaction);
                    modifiedCollections ??= Database.DocumentsStorage.GetCollections(context).Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
                }

                await WaitForIndexesAsync(Database, options.WaitForIndexesTimeout.Value,
                    options.SpecifiedIndexesQueryString, options.WaitForIndexThrow,
                    lastDocumentEtag, lastTombstoneEtag, modifiedCollections, token);
            }
        }


        private static void ThrowNotSupportedType(string contentType)
        {
            throw new InvalidOperationException($"The requested Content type '{contentType}' is not supported. Use 'application/json' or 'multipart/mixed'.");
        }

        private async Task ParseMultipart(DocumentsOperationContext context, MergedBatchCommand command, CancellationToken token)
        {
            var boundary = MultipartRequestHelper.GetBoundary(
                MediaTypeHeaderValue.Parse(HttpContext.Request.ContentType),
                MultipartRequestHelper.MultipartBoundaryLengthLimit);
            var reader = new MultipartReader(boundary, RequestBodyStream());
            for (var i = 0; i < int.MaxValue; i++)
            {
                var section = await reader.ReadNextSectionAsync(token).ConfigureAwait(false);
                if (section == null)
                    break;

                var bodyStream = GetBodyStream(section);
                if (i == 0)
                {
                    await BatchRequestParser.BuildCommandsAsync(context, command, bodyStream, Database, ServerStore, token);
                    continue;
                }

                if (command.AttachmentStreams == null)
                {
                    command.AttachmentStreams = new List<MergedBatchCommand.AttachmentStream>();
                    command.AttachmentStreamsTempFile = Database.DocumentsStorage.AttachmentsStorage.GetTempFile("batch");
                }

                var attachmentStream = new MergedBatchCommand.AttachmentStream
                {
                    Stream = command.AttachmentStreamsTempFile.StartNewStream()
                };
                attachmentStream.Hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, bodyStream, attachmentStream.Stream, token);
                await attachmentStream.Stream.FlushAsync(token);
                command.AttachmentStreams.Add(attachmentStream);
            }
        }

        public static async Task WaitForReplicationAsync(DocumentDatabase database, TimeSpan waitForReplicasTimeout, string numberOfReplicasStr, bool throwOnTimeoutInWaitForReplicas, string lastChangeVector)
        {
            int numberOfReplicasToWaitFor;

            if (numberOfReplicasStr == "majority")
            {
                numberOfReplicasToWaitFor = database.ReplicationLoader.GetMinNumberOfReplicas();
            }
            else
            {
                if (int.TryParse(numberOfReplicasStr, out numberOfReplicasToWaitFor) == false)
                    ThrowInvalidInteger("numberOfReplicasToWaitFor", numberOfReplicasStr);
            }

            var replicatedPast = await database.ReplicationLoader.WaitForReplicationAsync(
                numberOfReplicasToWaitFor,
                waitForReplicasTimeout,
                lastChangeVector);

            if (replicatedPast < numberOfReplicasToWaitFor && throwOnTimeoutInWaitForReplicas)
            {
                var message = $"Could not verify that etag {lastChangeVector} was replicated " +
                              $"to {numberOfReplicasToWaitFor} servers in {waitForReplicasTimeout}. " +
                              $"So far, it only replicated to {replicatedPast}";

                throw new RavenTimeoutException(message)
                {
                    FailImmediately = true
                };
            }
        }

        public static async Task WaitForIndexesAsync(DocumentDatabase database, TimeSpan timeout,
            List<string> specifiedIndexesQueryString, bool throwOnTimeout,
            long lastDocumentEtag, long lastTombstoneEtag, HashSet<string> modifiedCollections, CancellationToken token = default)
        {
            // waitForIndexesTimeout=timespan & waitForIndexThrow=false (default true)
            // waitForSpecificIndex=specific index1 & waitForSpecificIndex=specific index 2

            if (modifiedCollections.Count == 0)
                return;

            var indexesToWait = new List<WaitForIndexItem>();

            var indexesToCheck = GetImpactedIndexesToWaitForToBecomeNonStale(database, specifiedIndexesQueryString, modifiedCollections);

            if (indexesToCheck.Count == 0)
                return;

            var sp = Stopwatch.StartNew();

            bool needsServerContext = false;

            // we take the awaiter _before_ the indexing transaction happens,
            // so if there are any changes, it will already happen to it, and we'll
            // query the index again. This is important because of:
            // https://issues.hibernatingrhinos.com/issue/RavenDB-5576
            foreach (var index in indexesToCheck)
            {
                var indexToWait = new WaitForIndexItem
                {
                    Index = index,
                    IndexBatchAwaiter = index.GetIndexingBatchAwaiter(),
                    WaitForIndexing = new AsyncWaitForIndexing(sp, timeout, index)
                };

                indexesToWait.Add(indexToWait);

                needsServerContext |= index.Definition.HasCompareExchange;
            }

            var cutoffEtag = Math.Max(lastDocumentEtag, lastTombstoneEtag);

            while (true)
            {
                var hadStaleIndexes = false;

                using (var context = QueryOperationContext.Allocate(database, needsServerContext))
                using (context.OpenReadTransaction())
                {
                    for (var i = 0; i < indexesToWait.Count; i++)
                    {
                        var waitForIndexItem = indexesToWait[i];
                        if (waitForIndexItem.Index.IsStale(context, cutoffEtag) == false)
                            continue;

                        hadStaleIndexes = true;

                        await waitForIndexItem.WaitForIndexing.WaitForIndexingAsync(waitForIndexItem.IndexBatchAwaiter).WithCancellation(token);

                        if (waitForIndexItem.WaitForIndexing.TimeoutExceeded)
                        {
                            if (throwOnTimeout == false)
                                return;

                            ThrowTimeoutException(indexesToWait, i, sp, context, cutoffEtag);
                        }
                    }
                }

                if (hadStaleIndexes == false)
                    return;
            }
        }

        private static void ThrowTimeoutException(List<WaitForIndexItem> indexesToWait, int i, Stopwatch sp, QueryOperationContext context, long cutoffEtag)
        {
            var staleIndexes = new List<string>();
            var erroredIndexes = new List<string>();
            var pausedIndexes = new List<string>();

            for (var j = i; j < indexesToWait.Count; j++)
            {
                var index = indexesToWait[j].Index;

                if (index.State == IndexState.Error)
                {
                    erroredIndexes.Add(index.Name);
                }
                else if (index.Status == IndexRunningStatus.Paused)
                {
                    pausedIndexes.Add(index.Name);
                }

                if (index.IsStale(context, cutoffEtag))
                {
                    staleIndexes.Add(index.Name);
                }
            }

            var errorMessage = $"After waiting for {sp.Elapsed}, could not verify that all indexes has caught up with the changes as of etag: {cutoffEtag:#,#;;0}. " +
                               $"Total relevant indexes: {indexesToWait.Count}, total stale indexes: {staleIndexes.Count} ({string.Join(", ", staleIndexes)})";

            if (erroredIndexes.Count > 0)
            {
                errorMessage += $", total errored indexes: {erroredIndexes.Count} ({string.Join(", ", erroredIndexes)})";
            }

            if (pausedIndexes.Count > 0)
            {
                errorMessage += $", total paused indexes: {pausedIndexes.Count} ({string.Join(", ", pausedIndexes)})";
            }

            throw new RavenTimeoutException(errorMessage)
            {
                FailImmediately = true
            };
        }

        private static List<Index> GetImpactedIndexesToWaitForToBecomeNonStale(DocumentDatabase database, List<string> specifiedIndexesQueryString, HashSet<string> modifiedCollections)
        {
            var indexesToCheck = new List<Index>();

            if (specifiedIndexesQueryString is { Count: > 0 })
            {
                var specificIndexes = specifiedIndexesQueryString.ToHashSet();
                foreach (var index in database.IndexStore.GetIndexes())
                {
                    if (specificIndexes.Contains(index.Name))
                    {
                        if (index.WorksOnAnyCollection(modifiedCollections))
                            indexesToCheck.Add(index);
                    }
                }
            }
            else
            {
                foreach (var index in database.IndexStore.GetIndexes())
                {
                    if (index.State is IndexState.Disabled)
                        continue;

                    if (index.Collections.Contains(Constants.Documents.Collections.AllDocumentsCollection) ||
                        index.WorksOnAnyCollection(modifiedCollections))
                    {
                        indexesToCheck.Add(index);
                    }
                }
            }
            return indexesToCheck;
        }

        public abstract class TransactionMergedCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            protected readonly DocumentDatabase Database;
            public HashSet<string> ModifiedCollections;
            public string LastChangeVector;

            public long LastTombstoneEtag;
            public long LastDocumentEtag;

            public DynamicJsonArray Reply = new DynamicJsonArray();

            protected TransactionMergedCommand(DocumentDatabase database)
            {
                Database = database;
            }

            protected void AddPutResult(DocumentsStorage.PutOperationResults putResult)
            {
                LastChangeVector = putResult.ChangeVector;
                LastDocumentEtag = putResult.Etag;
                ModifiedCollections?.Add(putResult.Collection.Name);

                // Make sure all the metadata fields are always been add
                var putReply = new DynamicJsonValue
                {
                    ["Type"] = nameof(CommandType.PUT),
                    [Constants.Documents.Metadata.Id] = putResult.Id,
                    [Constants.Documents.Metadata.Collection] = putResult.Collection.Name,
                    [Constants.Documents.Metadata.ChangeVector] = putResult.ChangeVector,
                    [Constants.Documents.Metadata.LastModified] = putResult.LastModified
                };

                if (putResult.Flags != DocumentFlags.None)
                    putReply[Constants.Documents.Metadata.Flags] = putResult.Flags;

                Reply.Add(putReply);
            }

            protected void AddDeleteResult(DocumentsStorage.DeleteOperationResult? deleted, string id)
            {
                var reply = new DynamicJsonValue
                {
                    [nameof(BatchRequestParser.CommandData.Id)] = id,
                    [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.DELETE),
                    ["Deleted"] = deleted != null
                };

                if (deleted != null)
                {
                    if (deleted.Value.Collection != null)
                        ModifiedCollections?.Add(deleted.Value.Collection.Name);

                    LastTombstoneEtag = deleted.Value.Etag;
                    reply[nameof(BatchRequestParser.CommandData.ChangeVector)] = deleted.Value.ChangeVector;
                }

                Reply.Add(reply);
            }

            protected void DeleteWithPrefix(DocumentsOperationContext context, string id)
            {
                var deleteResults = Database.DocumentsStorage.DeleteDocumentsStartingWith(context, id);

                var deleted = deleteResults.Count > 0;
                if (deleted)
                {
                    LastChangeVector = deleteResults[deleteResults.Count - 1].ChangeVector;
                    for (var j = 0; j < deleteResults.Count; j++)
                    {
                        ModifiedCollections?.Add(deleteResults[j].Collection.Name);
                    }
                }

                Reply.Add(new DynamicJsonValue
                {
                    [nameof(BatchRequestParser.CommandData.Id)] = id,
                    [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.DELETE),
                    ["Deleted"] = deleted
                });
            }
        }

        public class ClusterTransactionMergedCommand : TransactionMergedCommand
        {
            private readonly List<ClusterTransactionCommand.SingleClusterDatabaseCommand> _batch;
            public readonly Dictionary<long, DynamicJsonArray> Replies = new Dictionary<long, DynamicJsonArray>();
            public readonly Dictionary<long, ClusterTransactionCommand.ClusterTransactionOptions> Options = new Dictionary<long, ClusterTransactionCommand.ClusterTransactionOptions>();

            public ClusterTransactionMergedCommand(DocumentDatabase database, List<ClusterTransactionCommand.SingleClusterDatabaseCommand> batch) : base(database)
            {
                _batch = batch;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var global = context.LastDatabaseChangeVector ??
                             (context.LastDatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context));
                var current = ChangeVectorUtils.GetEtagById(global, Database.DatabaseGroupId);

                Replies.Clear();
                Options.Clear();

                long lastIndexInBatch = 0L;

                foreach (var command in _batch)
                {
                    Replies.Add(command.Index, new DynamicJsonArray());
                    Reply = Replies[command.Index];

                    var commands = command.Commands;
                    var count = command.PreviousCount;
                    var options = Options[command.Index] = command.Options;

                    if (options.WaitForIndexesTimeout != null)
                    {
                        ModifiedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    }

                    if (commands != null)
                    {
                        foreach (var cmd in commands)
                        {
                            count++;
                            var changeVector = ChangeVectorUtils.GetClusterWideChangeVector(Database.DatabaseGroupId, count, options.DisableAtomicDocumentWrites == false, command.Index, Database.ClusterTransactionId);

                            switch (cmd.Type)
                            {
                                case CommandType.PUT:
                                    if (current < count)
                                    {
                                        // if the document came from a full backup it must have the same collection
                                        // the only thing that we update is the change vector
                                        if (cmd.FromBackup is not BackupKind.Full)
                                        {
                                            // delete the document to avoid exception if we put new document in a different collection.
                                            using (DocumentIdWorker.GetSliceFromId(context, cmd.Id, out Slice lowerId))
                                            {
                                                Database.DocumentsStorage.Delete(context, lowerId, cmd.Id, expectedChangeVector: null,
                                                    nonPersistentFlags: NonPersistentDocumentFlags.SkipRevisionCreation);
                                            }
                                        }

                                        var putResult = Database.DocumentsStorage.Put(context, cmd.Id, null, cmd.Document, changeVector: changeVector,
                                            flags: DocumentFlags.FromClusterTransaction);
                                        context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Id, cmd.Document.Size);
                                        AddPutResult(putResult);
                                    }
                                    else
                                    {
                                        try
                                        {
                                            var item = Database.DocumentsStorage.GetDocumentOrTombstone(context, cmd.Id);
                                            if (item.Missing)
                                            {
                                                AddPutResult(new DocumentsStorage.PutOperationResults
                                                {
                                                    ChangeVector = changeVector,
                                                    Id = cmd.Id,
                                                    LastModified = DateTime.UtcNow,
                                                    Collection = Database.DocumentsStorage.ExtractCollectionName(context, cmd.Document)
                                                });
                                                continue;
                                            }
                                            var collection = GetCollection(context, item);
                                            AddPutResult(new DocumentsStorage.PutOperationResults
                                            {
                                                ChangeVector = changeVector,
                                                Id = cmd.Id,
                                                Flags = item.Document?.Flags ?? item.Tombstone.Flags,
                                                LastModified = item.Document?.LastModified ?? item.Tombstone.LastModified,
                                                Collection = collection
                                            });
                                        }
                                        catch (DocumentConflictException)
                                        {
                                            AddPutResult(new DocumentsStorage.PutOperationResults
                                            {
                                                ChangeVector = changeVector,
                                                Id = cmd.Id,
                                                Collection = GetFirstConflictCollection(context, cmd)
                                            });
                                        }
                                    }

                                    break;

                                case CommandType.DELETE:
                                    if (current < count)
                                    {
                                        using (DocumentIdWorker.GetSliceFromId(context, cmd.Id, out Slice lowerId))
                                        {
                                            var deleteResult = Database.DocumentsStorage.Delete(context, lowerId, cmd.Id, null, changeVector: changeVector,
                                                documentFlags: DocumentFlags.FromClusterTransaction);
                                            AddDeleteResult(deleteResult, cmd.Id);
                                        }
                                    }
                                    else
                                    {
                                        try
                                        {
                                            var item = Database.DocumentsStorage.GetDocumentOrTombstone(context, cmd.Id);
                                            if (item.Missing)
                                            {
                                                AddDeleteResult(new DocumentsStorage.DeleteOperationResult
                                                {
                                                    ChangeVector = changeVector,
                                                    Collection = null
                                                }, cmd.Id);
                                                continue;
                                            }
                                            var collection = GetCollection(context, item);
                                            AddDeleteResult(new DocumentsStorage.DeleteOperationResult
                                            {
                                                ChangeVector = changeVector,
                                                Collection = collection
                                            }, cmd.Id);
                                        }
                                        catch (DocumentConflictException)
                                        {
                                            AddDeleteResult(new DocumentsStorage.DeleteOperationResult
                                            {
                                                ChangeVector = changeVector,
                                                Collection = GetFirstConflictCollection(context, cmd)
                                            }, cmd.Id);
                                        }
                                    }
                                    break;

                                default:
                                    throw new NotSupportedException($"{cmd.Type} is not supported in {nameof(ClusterTransactionMergedCommand)}.");
                            }
                        }
                    }

                    if (context.LastDatabaseChangeVector == null)
                    {
                        context.LastDatabaseChangeVector = global;
                    }

                    var updatedChangeVector = ChangeVectorUtils.TryUpdateChangeVector("RAFT", Database.DatabaseGroupId, count, global);

                    if (updatedChangeVector.IsValid)
                    {
                        context.LastDatabaseChangeVector = updatedChangeVector.ChangeVector;
                    }

                    lastIndexInBatch = long.Max(lastIndexInBatch, command.Index);
                }

                // set last cluster transaction index (persistent)
                var lastClusterTxIndex = DocumentsStorage.ReadLastCompletedClusterTransactionIndex(context.Transaction.InnerTransaction);
                if (lastIndexInBatch > lastClusterTxIndex)
                    Database.DocumentsStorage.SetLastCompletedClusterTransactionIndex(context, lastIndexInBatch);

                return Reply.Count;
            }

            private CollectionName GetCollection(DocumentsOperationContext context, DocumentsStorage.DocumentOrTombstone item)
            {
                return item.Document != null
                    ? Database.DocumentsStorage.ExtractCollectionName(context, item.Document.Data)
                    : Database.DocumentsStorage.ExtractCollectionName(context, item.Tombstone.Collection);
            }

            private CollectionName GetFirstConflictCollection(DocumentsOperationContext context, ClusterTransactionCommand.ClusterTransactionDataCommand cmd)
            {
                var conflicts = Database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, cmd.Id);
                if (conflicts.Count == 0)
                    return null;
                return Database.DocumentsStorage.ExtractCollectionName(context, conflicts[0].Collection);
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto<TTransaction>(TransactionOperationContext<TTransaction> context)
            {
                return new ClusterTransactionMergedCommandDto
                {
                    Batch = _batch
                };
            }
        }

        public class MergedBatchCommand : TransactionMergedCommand, IDisposable
        {
            public ArraySegment<BatchRequestParser.CommandData> ParsedCommands;
            public List<AttachmentStream> AttachmentStreams;
            public StreamsTempFile AttachmentStreamsTempFile;

            private Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>> _documentsToUpdateAfterAttachmentChange;
            private readonly List<IDisposable> _disposables = new List<IDisposable>();

            public bool IsClusterTransaction;

            public MergedBatchCommand(DocumentDatabase database) : base(database)
            {
            }

            public override string ToString()
            {
                var sb = new StringBuilder($"{ParsedCommands.Count} commands").AppendLine();
                if (AttachmentStreams != null)
                {
                    sb.AppendLine($"{AttachmentStreams.Count} attachment streams.");
                }

                foreach (var cmd in ParsedCommands)
                {
                    sb.Append("\t")
                        .Append(cmd.Type)
                        .Append(" ")
                        .Append(cmd.Id)
                        .AppendLine();
                }

                return sb.ToString();
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto<TTransaction>(TransactionOperationContext<TTransaction> context)
            {
                return new MergedBatchCommandDto
                {
                    ParsedCommands = ParsedCommands.ToArray(),
                    AttachmentStreams = AttachmentStreams
                };
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                if (IsClusterTransaction)
                {
                    Debug.Assert(false, "Shouldn't happen - cluster tx run via normal means");
                    return 0;// should never happened
                }
                Reply.Clear();
                _disposables.Clear();

                DocumentsStorage.PutOperationResults? lastPutResult = null;

                using IEnumerator<AttachmentStream> attachmentIterator = AttachmentStreams?.GetEnumerator();

                for (int i = ParsedCommands.Offset; i < ParsedCommands.Count; i++)
                {
                    var cmd = ParsedCommands.Array[i];

                    switch (cmd.Type)
                    {
                        case CommandType.PUT:

                            DocumentsStorage.PutOperationResults putResult;
                            try
                            {
                                var flags = DocumentFlags.None;

                                if (cmd.ForceRevisionCreationStrategy == ForceRevisionStrategy.Before)
                                // Note: we currently only handle 'Before'.
                                // Creating the revision 'After' will be done only upon customer demand.
                                {
                                    var existingDocument = Database.DocumentsStorage.Get(context, cmd.Id);
                                    if (existingDocument == null)
                                    {
                                        throw new InvalidOperationException($"Can't force revision creation - the document was not saved on the server yet. document: {cmd.Id}.");
                                    }

                                    // Force a revision (before applying new document changes..)
                                    Database.DocumentsStorage.RevisionsStorage.Put(context, existingDocument.Id,
                                                                                   existingDocument.Data.Clone(context),
                                                                                   existingDocument.Flags |= DocumentFlags.HasRevisions,
                                                                                   nonPersistentFlags: NonPersistentDocumentFlags.ForceRevisionCreation,
                                                                                   existingDocument.ChangeVector,
                                                                                   existingDocument.LastModified.Ticks);
                                    flags |= DocumentFlags.HasRevisions;
                                }

                                putResult = Database.DocumentsStorage.Put(context, cmd.Id, cmd.ChangeVector, cmd.Document,
                                    oldChangeVectorForClusterTransactionIndexCheck: cmd.OriginalChangeVector, flags: flags);
                            }
                            catch (Voron.Exceptions.VoronConcurrencyErrorException)
                            {
                                // RavenDB-10581 - If we have a concurrency error on "doc-id/"
                                // this means that we have existing values under the current etag
                                // we'll generate a new (random) id for them.

                                // The TransactionMerger will re-run us when we ask it to as a
                                // separate transaction
                                for (; i < ParsedCommands.Count; i++)
                                {
                                    cmd = ParsedCommands.Array[ParsedCommands.Offset + i];
                                    if (cmd.Type == CommandType.PUT && cmd.Id?.EndsWith(Database.IdentityPartsSeparator) == true)
                                    {
                                        cmd.Id = MergedPutCommand.GenerateNonConflictingId(Database, cmd.Id);
                                        RetryOnError = true;
                                    }
                                }
                                throw;
                            }

                            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(putResult.Id, cmd.Document.Size);
                            AddPutResult(putResult);
                            lastPutResult = putResult;
                            break;

                        case CommandType.PATCH:
                        case CommandType.BatchPATCH:
                            cmd.PatchCommand.ExecuteDirectly(context);

                            var lastChangeVector = cmd.PatchCommand.HandleReply(Reply, ModifiedCollections);

                            if (lastChangeVector != null)
                                LastChangeVector = lastChangeVector;

                            break;

                        case CommandType.JsonPatch:

                            cmd.JsonPatchCommand.ExecuteDirectly(context);

                            var lastChangeVectorJsonPatch = cmd.JsonPatchCommand.HandleReply(Reply, ModifiedCollections, Database);

                            if (lastChangeVectorJsonPatch != null)
                                LastChangeVector = lastChangeVectorJsonPatch;
                            break;

                        case CommandType.DELETE:
                            if (cmd.IdPrefixed == false)
                            {
                                var deleted = Database.DocumentsStorage.Delete(context, cmd.Id, cmd.ChangeVector);
                                AddDeleteResult(deleted, cmd.Id);
                            }
                            else
                            {
                                DeleteWithPrefix(context, cmd.Id);
                            }
                            break;

                        case CommandType.AttachmentPUT:
                            attachmentIterator.MoveNext();
                            var attachmentStream = attachmentIterator.Current;
                            var stream = attachmentStream.Stream;
                            _disposables.Add(stream);

                            var docId = cmd.Id;

                            EtlGetDocIdFromPrefixIfNeeded(ref docId, cmd, lastPutResult);

                            var attachmentPutResult = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, docId, cmd.Name,
                                cmd.ContentType, attachmentStream.Hash, cmd.ChangeVector, stream, updateDocument: false, extractCollectionName: ModifiedCollections is not null);
                            LastChangeVector = attachmentPutResult.ChangeVector;

                            var apReply = new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = attachmentPutResult.DocumentId,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.AttachmentPUT),
                                [nameof(BatchRequestParser.CommandData.Name)] = attachmentPutResult.Name,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = attachmentPutResult.ChangeVector,
                                [nameof(AttachmentDetails.Hash)] = attachmentPutResult.Hash,
                                [nameof(BatchRequestParser.CommandData.ContentType)] = attachmentPutResult.ContentType,
                                [nameof(AttachmentDetails.Size)] = attachmentPutResult.Size
                            };

                            if (attachmentPutResult.CollectionName != null)
                                ModifiedCollections?.Add(attachmentPutResult.CollectionName.Name);

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                            if (_documentsToUpdateAfterAttachmentChange.TryGetValue(docId, out var apReplies) == false)
                                _documentsToUpdateAfterAttachmentChange[docId] = apReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                            apReplies.Add((apReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));
                            Reply.Add(apReply);
                            break;

                        case CommandType.AttachmentDELETE:
                            Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, cmd.Id, cmd.Name, cmd.ChangeVector, out var collectionName, updateDocument: false, extractCollectionName: ModifiedCollections is not null);

                            if (collectionName != null)
                                ModifiedCollections?.Add(collectionName.Name);
                            
                            var adReply = new DynamicJsonValue
                            {
                                ["Type"] = nameof(CommandType.AttachmentDELETE),
                                [Constants.Documents.Metadata.Id] = cmd.Id,
                                ["Name"] = cmd.Name
                            };

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                            if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.Id, out var adReplies) == false)
                                _documentsToUpdateAfterAttachmentChange[cmd.Id] = adReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                            adReplies.Add((adReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));
                            Reply.Add(adReply);
                            break;

                        case CommandType.AttachmentMOVE:
                            var attachmentMoveOutput = Database.DocumentsStorage.AttachmentsStorage.MoveAttachment(context, cmd.Id, cmd.Name, cmd.DestinationId, cmd.DestinationName, cmd.ChangeVector, extractCollectionName: ModifiedCollections is not null);
                            var attachmentMoveResult = attachmentMoveOutput.Result;
                            
                            if (attachmentMoveOutput.DestinationCollectionName != null)
                                ModifiedCollections?.Add(attachmentMoveOutput.DestinationCollectionName.Name);
                            if (attachmentMoveOutput.SourceCollectionName != null)
                                ModifiedCollections?.Add(attachmentMoveOutput.SourceCollectionName.Name);

                            LastChangeVector = attachmentMoveResult.ChangeVector;

                            var amReply = new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.AttachmentMOVE),
                                [nameof(BatchRequestParser.CommandData.Name)] = cmd.Name,
                                [nameof(BatchRequestParser.CommandData.DestinationId)] = attachmentMoveResult.DocumentId,
                                [nameof(BatchRequestParser.CommandData.DestinationName)] = attachmentMoveResult.Name,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = attachmentMoveResult.ChangeVector,
                                [nameof(AttachmentDetails.Hash)] = attachmentMoveResult.Hash,
                                [nameof(BatchRequestParser.CommandData.ContentType)] = attachmentMoveResult.ContentType,
                                [nameof(AttachmentDetails.Size)] = attachmentMoveResult.Size
                            };

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                            if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.Id, out var amReplies) == false)
                                _documentsToUpdateAfterAttachmentChange[cmd.Id] = amReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                            amReplies.Add((amReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));

                            if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.DestinationId, out amReplies) == false)
                                _documentsToUpdateAfterAttachmentChange[cmd.DestinationId] = amReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                            amReplies.Add((amReply, nameof(Constants.Fields.CommandData.DestinationDocumentChangeVector)));

                            Reply.Add(amReply);
                            break;

                        case CommandType.AttachmentCOPY:
                            if (cmd.AttachmentType == 0)
                            {
                                // if attachment type is not sent, we fallback to default, which is Document
                                cmd.AttachmentType = AttachmentType.Document;
                            }
                            var attachmentCopyResult = Database.DocumentsStorage.AttachmentsStorage.CopyAttachment(context, cmd.Id, cmd.Name, cmd.DestinationId, cmd.DestinationName, cmd.ChangeVector, cmd.AttachmentType, extractCollectionName: ModifiedCollections is not null);

                            if (attachmentCopyResult.CollectionName != null)
                                ModifiedCollections?.Add(attachmentCopyResult.CollectionName.Name);
                                
                            LastChangeVector = attachmentCopyResult.ChangeVector;

                            var acReply = new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = attachmentCopyResult.DocumentId,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.AttachmentCOPY),
                                [nameof(BatchRequestParser.CommandData.Name)] = attachmentCopyResult.Name,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = attachmentCopyResult.ChangeVector,
                                [nameof(AttachmentDetails.Hash)] = attachmentCopyResult.Hash,
                                [nameof(BatchRequestParser.CommandData.ContentType)] = attachmentCopyResult.ContentType,
                                [nameof(AttachmentDetails.Size)] = attachmentCopyResult.Size
                            };

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new Dictionary<string, List<(DynamicJsonValue Reply, string FieldName)>>(StringComparer.OrdinalIgnoreCase);

                            if (_documentsToUpdateAfterAttachmentChange.TryGetValue(cmd.DestinationId, out var acReplies) == false)
                                _documentsToUpdateAfterAttachmentChange[cmd.DestinationId] = acReplies = new List<(DynamicJsonValue Reply, string FieldName)>();

                            acReplies.Add((acReply, nameof(Constants.Fields.CommandData.DocumentChangeVector)));
                            Reply.Add(acReply);
                            break;

                        case CommandType.TimeSeries:
                        case CommandType.TimeSeriesWithIncrements:
                            EtlGetDocIdFromPrefixIfNeeded(ref cmd.Id, cmd, lastPutResult);
                            var tsCmd = new TimeSeriesHandler.ExecuteTimeSeriesBatchCommand(Database, cmd.Id, cmd.TimeSeries, cmd.FromEtl);

                            tsCmd.ExecuteDirectly(context);

                            LastChangeVector = tsCmd.LastChangeVector;

                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = tsCmd.LastChangeVector,
                                [nameof(BatchRequestParser.CommandData.Type)] = cmd.Type,
                                //TODO: handle this
                                //[nameof(Constants.Fields.CommandData.DocumentChangeVector)] = tsCmd.LastDocumentChangeVector
                            });

                            if (tsCmd.DocCollection != null)
                                ModifiedCollections?.Add(tsCmd.DocCollection);

                            break;

                        case CommandType.TimeSeriesCopy:

                            var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(context, cmd.Id, cmd.Name, cmd.From ?? DateTime.MinValue, cmd.To ?? DateTime.MaxValue);

                            var destinationDocCollection = TimeSeriesHandler.ExecuteTimeSeriesBatchCommand.GetDocumentCollection(Database, context, cmd.DestinationId, fromEtl: false);

                            var cv = Database.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context,
                                    cmd.DestinationId,
                                    destinationDocCollection,
                                    cmd.DestinationName,
                                    reader.AllValues(),
                                    AppendOptionsForTimeSeriesCopy
                                );

                            LastChangeVector = cv;

                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = cmd.DestinationId,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = cv,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.TimeSeriesCopy),
                            });

                            ModifiedCollections?.Add(destinationDocCollection);

                            break;

                        case CommandType.Counters:
                            EtlGetDocIdFromPrefixIfNeeded(ref cmd.Counters.DocumentId, cmd, lastPutResult);

                            var counterBatchCmd = new CountersHandler.ExecuteCounterBatchCommand(Database, new CounterBatch
                            {
                                Documents = new List<DocumentCountersOperation> { cmd.Counters },
                                FromEtl = cmd.FromEtl
                            });
                            counterBatchCmd.ExecuteDirectly(context);

                            LastChangeVector = counterBatchCmd.LastChangeVector;

                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = counterBatchCmd.LastChangeVector,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.Counters),
                                [nameof(CountersDetail)] = counterBatchCmd.CountersDetail.ToJson(),
                                [nameof(Constants.Fields.CommandData.DocumentChangeVector)] = counterBatchCmd.LastDocumentChangeVector
                            });

                            if (counterBatchCmd.DocumentCollections != null)
                            {
                                foreach (var collection in counterBatchCmd.DocumentCollections)
                                {
                                    ModifiedCollections?.Add(collection);
                                }
                            }

                            break;

                        case CommandType.ForceRevisionCreation:
                            // Create a revision for an existing document (specified by the id) even if revisions settings are Not configured for the collection.
                            // Only one such revision will be created.
                            // i.e. If there is already an existing 'forced' revision to this document then we don't create another forced revision.

                            var existingDoc = Database.DocumentsStorage.Get(context, cmd.Id);
                            if (existingDoc == null)
                            {
                                throw new InvalidOperationException($"Failed to create revision for document {cmd.Id} because document doesn't exits.");
                            }

                            DynamicJsonValue forceRevisionReply;

                            // Note: here there is no point checking 'Before' or 'After' because if there were any changes then the forced revision is done from the PUT command....

                            bool revisionCreated = false;
                            var clonedDocData = existingDoc.Data.Clone(context);

                            if (existingDoc.Flags.Contain(DocumentFlags.HasRevisions) == false)
                            {
                                // When forcing a revision for a document that doesn't have revisions yet,
                                // we must add HasRevisions flag to the document and save (put)
                                existingDoc.Flags = existingDoc.Flags |= DocumentFlags.HasRevisions;

                                putResult = Database.DocumentsStorage.Put(context, existingDoc.Id,
                                                                         existingDoc.ChangeVector,
                                                                          clonedDocData,
                                                                          flags: existingDoc.Flags,
                                                                          nonPersistentFlags: NonPersistentDocumentFlags.SkipRevisionCreation);

                                existingDoc.ChangeVector = putResult.ChangeVector;
                                existingDoc.LastModified = putResult.LastModified;
                            }

                            // Create the revision. Revision will be created only if there isn't a revision with identical content for this document
                            revisionCreated = Database.DocumentsStorage.RevisionsStorage.Put(context, existingDoc.Id,
                                                                                         clonedDocData,
                                                                                         existingDoc.Flags,
                                                                                         nonPersistentFlags: NonPersistentDocumentFlags.ForceRevisionCreation,
                                                                                         existingDoc.ChangeVector,
                                                                                         existingDoc.LastModified.Ticks);
                            if (revisionCreated)
                            {
                                // Reply with new revision data
                                forceRevisionReply = new DynamicJsonValue
                                {
                                    ["RevisionCreated"] = true,
                                    ["Type"] = nameof(CommandType.ForceRevisionCreation),
                                    [Constants.Documents.Metadata.Id] = existingDoc.Id.ToString(), //We must not return to handlers memory allocated by merger.
                                    [Constants.Documents.Metadata.ChangeVector] = existingDoc.ChangeVector,
                                    [Constants.Documents.Metadata.LastModified] = existingDoc.LastModified,
                                    [Constants.Documents.Metadata.Flags] = existingDoc.Flags
                                };

                                LastChangeVector = existingDoc.ChangeVector;
                            }
                            else
                            {
                                // No revision was created
                                forceRevisionReply = new DynamicJsonValue
                                {
                                    ["RevisionCreated"] = false,
                                    ["Type"] = nameof(CommandType.ForceRevisionCreation)
                                };
                            }

                            Reply.Add(forceRevisionReply);
                            break;

                    }
                }

                if (_documentsToUpdateAfterAttachmentChange != null)
                {
                    foreach (var kvp in _documentsToUpdateAfterAttachmentChange)
                    {
                        var documentId = kvp.Key;
                        var changeVector = Database.DocumentsStorage.AttachmentsStorage.UpdateDocumentAfterAttachmentChange(context, documentId);

                        if (changeVector == null)
                            continue;

                        LastChangeVector = changeVector;

                        if (kvp.Value == null)
                            continue;

                        foreach (var tpl in kvp.Value)
                            tpl.Reply[tpl.FieldName] = changeVector;
                    }
                }
                return Reply.Count;
            }

            private void EtlGetDocIdFromPrefixIfNeeded(ref string docId, BatchRequestParser.CommandData cmd, DocumentsStorage.PutOperationResults? lastPutResult)
            {
                if (cmd.FromEtl == false || docId[^1] != Database.IdentityPartsSeparator)
                    return;
                // counter/time-series/attachments sent by Raven ETL, only prefix is defined

                if (lastPutResult == null)
                    ThrowUnexpectedOrderOfRavenEtlCommands();

                Debug.Assert(lastPutResult.HasValue && lastPutResult.Value.Id.StartsWith(docId));
                docId = lastPutResult.Value.Id;
            }

            public void Dispose()
            {
                if (ParsedCommands.Count == 0)
                    return;

                foreach (var disposable in _disposables)
                {
                    disposable?.Dispose();
                }

                foreach (var cmd in ParsedCommands)
                {
                    cmd.Document?.Dispose();
                }
                BatchRequestParser.ReturnBuffer(ParsedCommands);
                AttachmentStreamsTempFile?.Dispose();
                AttachmentStreamsTempFile = null;
            }

            public struct AttachmentStream
            {
                public string Hash;
                public Stream Stream;
            }

            private void ThrowUnexpectedOrderOfRavenEtlCommands()
            {
                throw new InvalidOperationException($"Unexpected order of commands sent by Raven ETL. {CommandType.AttachmentPUT} needs to be preceded by {CommandType.PUT}");
            }
        }

        private class WaitForIndexItem
        {
            public Index Index;
            public AsyncManualResetEvent.FrozenAwaiter IndexBatchAwaiter;
            public AsyncWaitForIndexing WaitForIndexing;
        }
    }

    public class ClusterTransactionMergedCommandDto : TransactionOperationsMerger.IReplayableCommandDto<BatchHandler.ClusterTransactionMergedCommand>
    {
        public List<ClusterTransactionCommand.SingleClusterDatabaseCommand> Batch { get; set; }

        public BatchHandler.ClusterTransactionMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var command = new BatchHandler.ClusterTransactionMergedCommand(database, Batch);
            return command;
        }
    }

    public class MergedBatchCommandDto : TransactionOperationsMerger.IReplayableCommandDto<BatchHandler.MergedBatchCommand>
    {
        public BatchRequestParser.CommandData[] ParsedCommands { get; set; }
        public List<BatchHandler.MergedBatchCommand.AttachmentStream> AttachmentStreams;

        public BatchHandler.MergedBatchCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            for (var i = 0; i < ParsedCommands.Length; i++)
            {
                if (ParsedCommands[i].Type != CommandType.PATCH)
                {
                    continue;
                }

                ParsedCommands[i].PatchCommand = new PatchDocumentCommand(
                    context: context,
                    id: ParsedCommands[i].Id,
                    expectedChangeVector: ParsedCommands[i].ChangeVector,
                    skipPatchIfChangeVectorMismatch: false,
                    patch: (ParsedCommands[i].Patch, ParsedCommands[i].PatchArgs),
                    patchIfMissing: (ParsedCommands[i].PatchIfMissing, ParsedCommands[i].PatchIfMissingArgs),
                    database: database,
                    createIfMissing: ParsedCommands[i].CreateIfMissing,
                    isTest: false,
                    debugMode: false,
                    collectResultsNeeded: true,
                    returnDocument: ParsedCommands[i].ReturnDocument

                );
            }

            var newCmd = new BatchHandler.MergedBatchCommand(database)
            {
                ParsedCommands = ParsedCommands,
                AttachmentStreams = AttachmentStreams
            };

            return newCmd;
        }
    }
}
