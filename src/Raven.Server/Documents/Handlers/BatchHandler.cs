﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Indexes;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Runtime.ExceptionServices;
using Raven.Client.Json;
using Raven.Server.Json;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Config.Categories;
using Raven.Server.Exceptions;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Handlers
{
    public class BatchHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_docs", "POST", AuthorizationStatus.ValidUser)]
        public async Task BulkDocs()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var command = new MergedBatchCommand(Database))
            {
                var contentType = HttpContext.Request.ContentType;
                if (contentType == null ||
                    contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
                {
                    await BatchRequestParser.BuildCommandsAsync(context, command, RequestBodyStream(), Database, ServerStore);
                }
                else if (contentType.StartsWith("multipart/mixed", StringComparison.OrdinalIgnoreCase) ||
                    contentType.StartsWith("multipart/form-data", StringComparison.OrdinalIgnoreCase))
                {
                    await ParseMultipart(context, command);
                }
                else
                    ThrowNotSupportedType(contentType);

                var waitForIndexesTimeout = GetTimeSpanQueryString("waitForIndexesTimeout", required: false);
                var waitForIndexThrow = GetBoolValueQueryString("waitForIndexThrow", required: false) ?? true;
                var specifiedIndexesQueryString = HttpContext.Request.Query["waitForSpecificIndex"];

                if (command.IsClusterTransaction)
                {
                    if (Server.Configuration.Core.FeaturesAvailability == FeaturesAvailability.Stable)
                        FeaturesAvailabilityException.Throw("Cluster Transactions");

                    using (Database.ClusterTransactionWaiter.CreateTask(out var taskId))
                    {
                        // Since this is a cluster transaction we are not going to wait for the write assurance of the replication.
                        // Because in any case the user will get a raft index to wait upon on his next request.
                        var options = new ClusterTransactionCommand.ClusterTransactionOptions(taskId)
                        {
                            WaitForIndexesTimeout = waitForIndexesTimeout,
                            WaitForIndexThrow = waitForIndexThrow,
                            SpecifiedIndexesQueryString = specifiedIndexesQueryString.Count > 0 ? specifiedIndexesQueryString.ToList() : null,
                        };
                        await HandleClusterTransaction(context, command, options);
                    }
                    return;
                }

                if (waitForIndexesTimeout != null)
                    command.ModifiedCollections = new HashSet<string>();

                try
                {
                    await Database.TxMerger.Enqueue(command);
                    command?.ExceptionDispatchInfo?.Throw();
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
                    await WaitForIndexesAsync(ContextPool, Database, waitForIndexesTimeout.Value, specifiedIndexesQueryString.ToList(), waitForIndexThrow,
                        command.LastChangeVector, command.LastTombstoneEtag, command.ModifiedCollections);
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(BatchCommandResult.Results)] = command.Reply
                    });
                }
            }
        }

        public class ClusterTransactionCompletionResult
        {
            public Task IndexTask;
            public DynamicJsonArray Array;
        }

        private async Task HandleClusterTransaction(DocumentsOperationContext context, MergedBatchCommand command, ClusterTransactionCommand.ClusterTransactionOptions options)
        {
            var clusterTransactionCommand = new ClusterTransactionCommand(Database.Name, command.ParsedCommands, options);
            var result = await ServerStore.SendToLeaderAsync(clusterTransactionCommand);

            if (result.Result is List<string> errors)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
                throw new ConcurrencyException($"Failed to execute cluster transaction due to the following issues: {string.Join(Environment.NewLine, errors)}");
            }

            var array = new DynamicJsonArray();
            if (clusterTransactionCommand.DatabaseCommandsCount > 0)
            {
                var reply = (ClusterTransactionCompletionResult)await Database.ClusterTransactionWaiter.WaitForResults(options.TaskId, HttpContext.RequestAborted);
                if (reply.IndexTask != null)
                {
                    await reply.IndexTask;
                }

                array = reply.Array;
            }
            else
            {
                // wait for the command to be applied on this node (batch of cmpxchng ops only)
                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, result.Index);
            }

            foreach (var clusterCommands in clusterTransactionCommand.ClusterCommands)
            {
                array.Add(new DynamicJsonValue
                {
                    ["Type"] = clusterCommands.Type,
                    ["Key"] = clusterCommands.Id,
                    ["Index"] = result.Index
                });
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(BatchCommandResult.Results)] = array,
                    [nameof(BatchCommandResult.TransactionIndex)] = result.Index
                });
            }
        }

        private static void ThrowNotSupportedType(string contentType)
        {
            throw new InvalidOperationException($"The requested Content type '{contentType}' is not supported. Use 'application/json' or 'multipart/mixed'.");
        }

        private async Task ParseMultipart(DocumentsOperationContext context, MergedBatchCommand command)
        {
            var boundary = MultipartRequestHelper.GetBoundary(
                MediaTypeHeaderValue.Parse(HttpContext.Request.ContentType),
                MultipartRequestHelper.MultipartBoundaryLengthLimit);
            var reader = new MultipartReader(boundary, RequestBodyStream());
            for (var i = 0; i < int.MaxValue; i++)
            {
                var section = await reader.ReadNextSectionAsync().ConfigureAwait(false);
                if (section == null)
                    break;

                var bodyStream = GetBodyStream(section);
                if (i == 0)
                {
                    await BatchRequestParser.BuildCommandsAsync(context, command, bodyStream, Database, ServerStore);
                    continue;
                }

                if (command.AttachmentStreams == null)
                {
                    command.AttachmentStreams = new Queue<MergedBatchCommand.AttachmentStream>();
                    command.AttachmentStreamsTempFile = Database.DocumentsStorage.AttachmentsStorage.GetTempFile("batch");
                }

                var attachmentStream = new MergedBatchCommand.AttachmentStream
                {
                    Stream = command.AttachmentStreamsTempFile.StartNewStream()
                };
                attachmentStream.Hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, bodyStream, attachmentStream.Stream, Database.DatabaseShutdown);
                attachmentStream.Stream.Flush();
                command.AttachmentStreams.Enqueue(attachmentStream);
            }
        }

        public static async Task WaitForReplicationAsync(DocumentDatabase database, TimeSpan waitForReplicasTimeout, string numberOfReplicasStr, bool throwOnTimeoutInWaitForReplicas, string lastChangeVector)
        {
            int numberOfReplicasToWaitFor;

            if (numberOfReplicasStr == "majority")
            {
                numberOfReplicasToWaitFor = database.ReplicationLoader.GetSizeOfMajority();
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

                throw new TimeoutException(message);
            }
        }

        public static async Task WaitForIndexesAsync(DocumentsContextPool contextPool, DocumentDatabase database, TimeSpan timeout,
            List<string> specifiedIndexesQueryString, bool throwOnTimeout,
            string lastChangeVector, long lastTombstoneEtag, HashSet<string> modifiedCollections)
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

            // we take the awaiter _before_ the indexing transaction happens, 
            // so if there are any changes, it will already happen to it, and we'll 
            // query the index again. This is important because of: 
            // http://issues.hibernatingrhinos.com/issue/RavenDB-5576
            foreach (var index in indexesToCheck)
            {
                var indexToWait = new WaitForIndexItem
                {
                    Index = index,
                    IndexBatchAwaiter = index.GetIndexingBatchAwaiter(),
                    WaitForIndexing = new AsyncWaitForIndexing(sp, timeout, index)
                };

                indexesToWait.Add(indexToWait);
            }

            using (contextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                while (true)
                {
                    var hadStaleIndexes = false;

                    using (context.OpenReadTransaction())
                    {
                        foreach (var waitForIndexItem in indexesToWait)
                        {
                            var lastEtag = lastChangeVector != null ? ChangeVectorUtils.GetEtagById(lastChangeVector, database.DbBase64Id) : 0;

                            var cutoffEtag = Math.Max(lastEtag, lastTombstoneEtag);

                            if (waitForIndexItem.Index.IsStale(context, cutoffEtag) == false)
                                continue;

                            hadStaleIndexes = true;

                            await waitForIndexItem.WaitForIndexing.WaitForIndexingAsync(waitForIndexItem.IndexBatchAwaiter);

                            if (waitForIndexItem.WaitForIndexing.TimeoutExceeded && throwOnTimeout)
                            {
                                throw new TimeoutException(
                                    $"After waiting for {sp.Elapsed}, could not verify that {indexesToCheck.Count} " +
                                    $"indexes has caught up with the changes as of etag: {cutoffEtag}");
                            }
                        }
                    }

                    if (hadStaleIndexes == false)
                        return;
                }
            }
        }

        private static List<Index> GetImpactedIndexesToWaitForToBecomeNonStale(DocumentDatabase database, List<string> specifiedIndexesQueryString, HashSet<string> modifiedCollections)
        {
            var indexesToCheck = new List<Index>();

            if (specifiedIndexesQueryString.Count > 0)
            {
                var specificIndexes = specifiedIndexesQueryString.ToHashSet();
                foreach (var index in database.IndexStore.GetIndexes())
                {
                    if (specificIndexes.Contains(index.Name))
                    {
                        if (index.Collections.Count == 0 || index.Collections.Overlaps(modifiedCollections))
                            indexesToCheck.Add(index);
                    }
                }
            }
            else
            {
                foreach (var index in database.IndexStore.GetIndexes())
                {
                    if (index.Collections.Contains(Constants.Documents.Collections.AllDocumentsCollection) ||
                        index.Collections.Overlaps(modifiedCollections) ||
                        index.Collections.Count == 0)
                    {
                        indexesToCheck.Add(index);
                    }

                }
            }
            return indexesToCheck;
        }


        public abstract class TranscationMergedCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            protected readonly DocumentDatabase Database;
            public HashSet<string> ModifiedCollections;
            public string LastChangeVector;
            public long LastTombstoneEtag;

            public DynamicJsonArray Reply = new DynamicJsonArray();

            protected TranscationMergedCommand(DocumentDatabase database)
            {
                Database = database;
            }

            protected void AddPutResult(DocumentsStorage.PutOperationResults putResult)
            {
                LastChangeVector = putResult.ChangeVector;
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
                    LastTombstoneEtag = deleted.Value.Etag;
                    ModifiedCollections?.Add(deleted.Value.Collection.Name);
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

        public class ClusterTransactionMergedCommand : TranscationMergedCommand
        {
            private readonly BlittableJsonReaderArray _commands;
            private long _count;
            public ClusterTransactionCommand.ClusterTransactionOptions Options { get; }

            public ClusterTransactionMergedCommand(DocumentDatabase database, ClusterTransactionCommand.SingleClusterDatabaseCommand command) : base(database)
            {
                _commands = command.Commands;
                _count = command.PreviousCount;
                Options = command.Options;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                if (Database.ServerStore.Configuration.Core.FeaturesAvailability == FeaturesAvailability.Stable)
                    FeaturesAvailabilityException.Throw("Cluster Transactions");

                var global = DocumentsStorage.GetDatabaseChangeVector(context);
                var dbGrpId = Database.DatabaseGroupId;
                var current = ChangeVectorUtils.GetEtagById(global, dbGrpId);

                if (Options.WaitForIndexesTimeout != null)
                {
                    ModifiedCollections = new HashSet<string>();
                }

                if (_commands != null)
                {
                    foreach (BlittableJsonReaderObject blittableCommand in _commands)
                    {
                        _count++;
                        var changeVector = $"RAFT:{_count}-{dbGrpId}";
                        var cmd = JsonDeserializationServer.ClusterTransactionDataCommand(blittableCommand);

                        switch (cmd.Type)
                        {
                            case CommandType.PUT:
                                if (current < _count)
                                {
                                    var putResult = Database.DocumentsStorage.Put(context, cmd.Id, null, cmd.Document.Clone(context), changeVector: changeVector,
                                        flags: DocumentFlags.FromClusterTransaction);
                                    context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Id, cmd.Document.Size);
                                    AddPutResult(putResult);
                                }
                                else
                                {
                                    try
                                    {
                                        var item = Database.DocumentsStorage.GetDocumentOrTombstone(context, cmd.Id);
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
                                if (current < _count)
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

                var result = ChangeVectorUtils.TryUpdateChangeVector("RAFT", dbGrpId, _count, context.LastDatabaseChangeVector);
                if (result.IsValid)
                {
                    context.LastDatabaseChangeVector = result.ChangeVector;
                }

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
        }

        public class MergedBatchCommand : TranscationMergedCommand, IDisposable
        {
            public ArraySegment<BatchRequestParser.CommandData> ParsedCommands;
            public Queue<AttachmentStream> AttachmentStreams;
            public StreamsTempFile AttachmentStreamsTempFile;

            private HashSet<string> _documentsToUpdateAfterAttachmentChange;
            private readonly List<IDisposable> _disposables = new List<IDisposable>();
            public ExceptionDispatchInfo ExceptionDispatchInfo;

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

            private bool CanAvoidThrowingToMerger(Exception e, int commandOffset)
            {
                // if a concurrency exception has been thrown, because the user passed a change vector,
                // we need to check if we are on the very first command and can abort immediately without
                // having the transaction merger try to run the transactions again
                if (commandOffset == ParsedCommands.Offset)
                {
                    ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
                    return true;
                }

                return false;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                if (IsClusterTransaction)
                {
                    Debug.Assert(false, "Shouldn't happen - cluster tx run via normal means");
                    return 0;// should never happened
                }

                _disposables.Clear();

                DocumentsStorage.PutOperationResults? lastPutResult = null;

                for (int i = ParsedCommands.Offset; i < ParsedCommands.Count; i++)
                {
                    var cmd = ParsedCommands.Array[ParsedCommands.Offset + i];

                    switch (cmd.Type)
                    {
                        case CommandType.PUT:

                            DocumentsStorage.PutOperationResults putResult;
                            try
                            {
                                putResult = Database.DocumentsStorage.Put(context, cmd.Id, cmd.ChangeVector, cmd.Document);
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
                                    if (cmd.Type == CommandType.PUT && cmd.Id?.EndsWith('/') == true)
                                    {
                                        cmd.Id = MergedPutCommand.GenerateNonConflictingId(Database, cmd.Id);
                                        RetryOnError = true;
                                    }
                                }
                                throw;
                            }
                            catch (ConcurrencyException e) when (CanAvoidThrowingToMerger(e, i))
                            {
                                return 0;
                            }
                            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Id, cmd.Document.Size);
                            AddPutResult(putResult);
                            lastPutResult = putResult;
                            break;
                        case CommandType.PATCH:
                            try
                            {
                                cmd.PatchCommand.Execute(context);
                            }
                            catch (ConcurrencyException e) when (CanAvoidThrowingToMerger(e, i))
                            {
                                return 0;
                            }

                            var patchResult = cmd.PatchCommand.PatchResult;
                            if (patchResult.ModifiedDocument != null)
                                context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Id, patchResult.ModifiedDocument.Size);

                            if (patchResult.ChangeVector != null)
                                LastChangeVector = patchResult.ChangeVector;

                            if (patchResult.Collection != null)
                                ModifiedCollections?.Add(patchResult.Collection);

                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = patchResult.ChangeVector,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.PATCH),
                                ["PatchStatus"] = patchResult.Status.ToString(),
                                ["Debug"] = patchResult.Debug
                            });
                            break;
                        case CommandType.DELETE:
                            if (cmd.IdPrefixed == false)
                            {
                                DocumentsStorage.DeleteOperationResult? deleted;
                                try
                                {
                                    deleted = Database.DocumentsStorage.Delete(context, cmd.Id, cmd.ChangeVector);
                                }
                                catch (ConcurrencyException e) when (CanAvoidThrowingToMerger(e, i))
                                {
                                    return 0;
                                }
                                AddDeleteResult(deleted, cmd.Id);
                            }
                            else
                            {
                                DeleteWithPrefix(context, cmd.Id);
                            }
                            break;
                        case CommandType.AttachmentPUT:
                            var attachmentStream = AttachmentStreams.Dequeue();
                            var stream = attachmentStream.Stream;
                            _disposables.Add(stream);

                            var docId = cmd.Id;

                            if (docId[docId.Length - 1] == '/')
                            {
                                // attachment sent by Raven ETL, only prefix is defined

                                if (lastPutResult == null)
                                    ThrowUnexpectedOrderOfRavenEtlCommands();

                                Debug.Assert(lastPutResult.Value.Id.StartsWith(docId));

                                docId = lastPutResult.Value.Id;
                            }

                            var attachmentPutResult = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, docId, cmd.Name,
                                cmd.ContentType, attachmentStream.Hash, cmd.ChangeVector, stream, updateDocument: false);
                            LastChangeVector = attachmentPutResult.ChangeVector;

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            _documentsToUpdateAfterAttachmentChange.Add(docId);

                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = attachmentPutResult.DocumentId,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.AttachmentPUT),
                                [nameof(BatchRequestParser.CommandData.Name)] = attachmentPutResult.Name,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = attachmentPutResult.ChangeVector,
                                [nameof(AttachmentDetails.Hash)] = attachmentPutResult.Hash,
                                [nameof(BatchRequestParser.CommandData.ContentType)] = attachmentPutResult.ContentType,
                                [nameof(AttachmentDetails.Size)] = attachmentPutResult.Size
                            });

                            break;
                        case CommandType.AttachmentDELETE:
                            Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, cmd.Id, cmd.Name, cmd.ChangeVector, updateDocument: false);

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            _documentsToUpdateAfterAttachmentChange.Add(cmd.Id);

                            Reply.Add(new DynamicJsonValue
                            {
                                ["Type"] = nameof(CommandType.AttachmentDELETE),
                                [Constants.Documents.Metadata.Id] = cmd.Id,
                                ["Name"] = cmd.Name
                            });

                            break;
                        case CommandType.AttachmentMOVE:
                            var attachmentRenameResult = Database.DocumentsStorage.AttachmentsStorage.MoveAttachment(context, cmd.Id, cmd.Name, cmd.DestinationId, cmd.DestinationName, cmd.ChangeVector);

                            LastChangeVector = attachmentRenameResult.ChangeVector;

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            _documentsToUpdateAfterAttachmentChange.Add(cmd.Id);
                            _documentsToUpdateAfterAttachmentChange.Add(cmd.DestinationId);

                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = attachmentRenameResult.DocumentId,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.AttachmentMOVE),
                                [nameof(BatchRequestParser.CommandData.Name)] = attachmentRenameResult.Name,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = attachmentRenameResult.ChangeVector
                            });
                            break;
                        case CommandType.AttachmentCOPY:
                            var attachmentCopyResult = Database.DocumentsStorage.AttachmentsStorage.CopyAttachment(context, cmd.Id, cmd.Name, cmd.DestinationId, cmd.DestinationName, cmd.ChangeVector);

                            LastChangeVector = attachmentCopyResult.ChangeVector;

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            _documentsToUpdateAfterAttachmentChange.Add(cmd.DestinationId);

                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = attachmentCopyResult.DocumentId,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.AttachmentCOPY),
                                [nameof(BatchRequestParser.CommandData.Name)] = attachmentCopyResult.Name,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = attachmentCopyResult.ChangeVector
                            });
                            break;
                        case CommandType.Counters:

                            var counterDocId = cmd.Counters.DocumentId;

                            if (cmd.FromEtl && counterDocId[counterDocId.Length - 1] == '/')
                            {
                                // counter sent by Raven ETL, only prefix is defined

                                if (lastPutResult == null)
                                    ThrowUnexpectedOrderOfRavenEtlCommands();

                                Debug.Assert(lastPutResult.Value.Id.StartsWith(counterDocId));

                                cmd.Counters.DocumentId = lastPutResult.Value.Id;
                            }

                            var counterBatchCmd = new CountersHandler.ExecuteCounterBatchCommand(Database, new CounterBatch
                            {
                                Documents = new List<DocumentCountersOperation> { cmd.Counters },
                                FromEtl = cmd.FromEtl
                            });
                            try
                            {
                                counterBatchCmd.Execute(context);
                            }
                            catch (DocumentDoesNotExistException e) when (CanAvoidThrowingToMerger(e, i))
                            {
                                return 0;
                            }

                            LastChangeVector = counterBatchCmd.LastChangeVector;
                            Reply.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                [nameof(BatchRequestParser.CommandData.ChangeVector)] = counterBatchCmd.LastChangeVector,
                                [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.Counters),
                                [nameof(CountersDetail)] = counterBatchCmd.CountersDetail.ToJson(),
                            });
                            break;
                    }
                }

                if (_documentsToUpdateAfterAttachmentChange != null)
                {
                    foreach (var documentId in _documentsToUpdateAfterAttachmentChange)
                    {
                        var changeVector = Database.DocumentsStorage.AttachmentsStorage.UpdateDocumentAfterAttachmentChange(context, documentId);
                        if (changeVector != null)
                            LastChangeVector = changeVector;
                    }
                }
                return Reply.Count;
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
                    if (cmd.PatchCommand != null)
                        cmd.PatchCommand.Dispose();
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

}
