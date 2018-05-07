using System;
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
using Voron.Exceptions;
using System.Runtime.ExceptionServices;
using Raven.Client.Json;
using Raven.Server.Json;using Raven.Client.Documents.Operations.Counters;using Raven.Server.Json;using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
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
                var isClusterTransaction = false;
                var contentType = HttpContext.Request.ContentType;
                if (contentType == null ||
                    contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
                {
                    isClusterTransaction = await BatchRequestParser.BuildCommandsAsync(context, command, RequestBodyStream(), Database, ServerStore);
                }
                else if (contentType.StartsWith("multipart/mixed", StringComparison.OrdinalIgnoreCase) ||
                    contentType.StartsWith("multipart/form-data", StringComparison.OrdinalIgnoreCase))
                {
                    isClusterTransaction = await ParseMultipart(context, command);
                }
                else
                    ThrowNotSupportedType(contentType);

                var waitForIndexesTimeout = GetTimeSpanQueryString("waitForIndexesTimeout", required: false);
                var waitForIndexThrow = GetBoolValueQueryString("waitForIndexThrow", required: false) ?? true;
                var specifiedIndexesQueryString = HttpContext.Request.Query["waitForSpecificIndex"];

                var waitForReplicasTimeout = GetTimeSpanQueryString("waitForReplicasTimeout", required: false);
                var numberOfReplicasStr = GetStringQueryString("numberOfReplicasToWaitFor", required: false) ?? "1";
                var throwOnTimeoutInWaitForReplicas = GetBoolValueQueryString("throwOnTimeoutInWaitForReplicas", required: false) ?? true;

                if (isClusterTransaction)
                {
                    var options = new ClusterTransactionCommand.ClusterTransactionOptions
                    {
                        WaitForIndexesTimeout = waitForIndexesTimeout,
                        WaitForIndexThrow = waitForIndexThrow,
                        SpecifiedIndexesQueryString = specifiedIndexesQueryString.Count > 0 ? specifiedIndexesQueryString.ToList() : null,

                        WaitForReplicasTimeout = waitForReplicasTimeout,
                        NumberOfReplicas = numberOfReplicasStr,
                        ThrowOnTimeoutInWaitForReplicas = throwOnTimeoutInWaitForReplicas
                    };
                    var result = await HandleClusterTransaction(ContextPool, command, options);
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(BatchCommandResult.Results)] = result.Reply,
                            [nameof(BatchCommandResult.TransactionIndex)] = result.Index
                        });
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

                if (waitForReplicasTimeout != null)
                {
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
            public Task ReplicationTask;
            public Task IndexTask;
            public bool Skipped;
            public DynamicJsonArray Array;
        }

        private async Task<(DynamicJsonArray Reply, long Index)> HandleClusterTransaction(DocumentsContextPool pool, MergedBatchCommand command, ClusterTransactionCommand.ClusterTransactionOptions options)
        {
            var clusterTransactionCommand =
                new ClusterTransactionCommand(Database.Name, command.ParsedCommands, options);
            var result = await ServerStore.SendToLeaderAsync(clusterTransactionCommand);

            if (result.Result is List<string> errors)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
                throw new ConcurrencyException($"Failed to execute cluster transaction due to the following issues: {string.Join(Environment.NewLine, errors)}");
            }

            var reply = (ClusterTransactionCompletionResult)await Database.ClusterTransactionWaiter.WaitForResult(result.Index, HttpContext.RequestAborted);
            if (reply.Skipped)
            {
                using (pool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var databaseCommands in clusterTransactionCommand.DatabaseCommands)
                    {
                        var tuple = Database.DocumentsStorage.GetDocumentOrTombstone(context, databaseCommands.Id, false);
                        var document = tuple.Document;
                        var tombstone = tuple.Tombstone;

                        if (document != null)
                        {
                            var putReply = new DynamicJsonValue
                            {
                                ["Type"] = databaseCommands.Type,
                                [Constants.Documents.Metadata.Id] = document.Id,
                                [Constants.Documents.Metadata.Collection] = Database.DocumentsStorage.ExtractCollectionName(context, document.Data).Name,
                                [Constants.Documents.Metadata.ChangeVector] = document.ChangeVector,
                                [Constants.Documents.Metadata.LastModified] = document.LastModified
                            };

                            if (document.Flags != DocumentFlags.None)
                                putReply[Constants.Documents.Metadata.Flags] = document.Flags;

                            reply.Array.Add(putReply);
                            continue;
                        }

                        if (tombstone != null)
                        {
                            reply.Array.Add(new DynamicJsonValue
                            {
                                [nameof(BatchRequestParser.CommandData.Id)] = databaseCommands.Id,
                                [nameof(BatchRequestParser.CommandData.Type)] = databaseCommands.Type,
                                ["Deleted"] = true
                            });
                            continue;
                        }

                        reply.Array.Add(new DynamicJsonValue
                        {
                            [nameof(BatchRequestParser.CommandData.Id)] = databaseCommands.Id,
                            [nameof(BatchRequestParser.CommandData.Type)] = databaseCommands.Type,
                            ["Deleted"] = false
                        });
                    }
                }
            }
            foreach (var clusterCommands in clusterTransactionCommand.ClusterCommands)
            {
                reply.Array.Add(new DynamicJsonValue
                {
                    ["Type"] = clusterCommands.Type,
                    ["Key"] = clusterCommands.Key,
                    ["Index"] = result.Index
                });
            }

            if (reply.IndexTask != null)
            {
                await reply.IndexTask;
            }
            if (reply.ReplicationTask != null)
            {
                await reply.ReplicationTask;
            }

            return (reply.Array , result.Index);
        }

        private static void ThrowNotSupportedType(string contentType)
        {
            throw new InvalidOperationException($"The requested Content type '{contentType}' is not supported. Use 'application/json' or 'multipart/mixed'.");
        }

        private async Task<bool> ParseMultipart(DocumentsOperationContext context, MergedBatchCommand command)
        {
            var boundary = MultipartRequestHelper.GetBoundary(
                MediaTypeHeaderValue.Parse(HttpContext.Request.ContentType),
                MultipartRequestHelper.MultipartBoundaryLengthLimit);
            var reader = new MultipartReader(boundary, RequestBodyStream());
            var isClusterTransaction = false;
            for (var i = 0; i < int.MaxValue; i++)
            {
                var section = await reader.ReadNextSectionAsync().ConfigureAwait(false);
                if (section == null)
                    break;

                var bodyStream = GetBodyStream(section);
                if (i == 0)
                {
                    isClusterTransaction |= await BatchRequestParser.BuildCommandsAsync(context, command, bodyStream, Database, ServerStore);
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
            return isClusterTransaction;
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
                if (deleted != null)
                {
                    LastTombstoneEtag = deleted.Value.Etag;
                    ModifiedCollections?.Add(deleted.Value.Collection.Name);
                }

                Reply.Add(new DynamicJsonValue
                {
                    [nameof(BatchRequestParser.CommandData.Id)] = id,
                    [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.DELETE),
                    ["Deleted"] = deleted != null
                });
            }
        }

        public class ClusterTransactionMergedCommand : TranscationMergedCommand
        {
            private readonly TransactionOperationContext _serverContext;
            private readonly long _index;
            public bool Skipped { get; private set; }
            public ClusterTransactionCommand.ClusterTransactionOptions Options { get; private set; }

            public ClusterTransactionMergedCommand(DocumentDatabase database, TransactionOperationContext serverContext, long index) : base(database)
            {
                _serverContext = serverContext;
                _index = index;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                var global = DocumentsStorage.GetDatabaseChangeVector(context);
                var clusterId = Database.ServerStore.Engine.ClusterBase64Id;
                var currentRaftIndex = ChangeVectorUtils.GetEtagById(global, clusterId);

                // Check if we already have the documents of this cluster transaction.
                if (currentRaftIndex >= _index)
                {
                    Skipped = true;
                    return 0;
                }

                using (_serverContext.OpenReadTransaction())
                {
                    var first = Math.Max(ClusterTransactionCommand.ReadFirstIndex(_serverContext, Database.Name), currentRaftIndex + 1);

                    for (var i = first; i <= _index; i++)
                    {
                        var changeVectorElement = $"RAFT:{i}-{clusterId}";
                        var command = ClusterTransactionCommand.ReadCommandsBatch(_serverContext, Database.Name, i);

                        if (command.Commands == null)
                        {
                            continue;
                        }

                        Options = command.Options;
                        if (Options?.WaitForIndexesTimeout.HasValue == true)
                        {
                            // overwrite the existing one.
                            ModifiedCollections = new HashSet<string>();
                        }

                        foreach (BlittableJsonReaderObject blittableCommand in command.Commands)
                        {
                            var docCommand = JsonDeserializationServer.DocumentCommand(blittableCommand);
                            var cmd = new BatchRequestParser.CommandData
                            {
                                Id = docCommand.Id,
                                Type = docCommand.Type,
                                Document = docCommand.Document.Clone(context)
                            };

                            switch (cmd.Type)
                            {
                                case CommandType.PUT:
                                    var putResult = Database.DocumentsStorage.Put(context, cmd.Id, null, cmd.Document, changeVectorElement: changeVectorElement);
                                    context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Id, cmd.Document.Size);
                                    AddPutResult(putResult);
                                    break;
                                case CommandType.DELETE:
                                    var deleteResult = Database.DocumentsStorage.Delete(context, cmd.Id, null, changeVectorElement: changeVectorElement);
                                    AddDeleteResult(deleteResult, cmd.Id);
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                }

                return Reply.Count;
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

            private bool CanAvoidThrowingToMerger(ConcurrencyException e, int commandOffset)
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
                _disposables.Clear();
var counterBatch = new CounterBatch();                for (int i = ParsedCommands.Offset; i < ParsedCommands.Count; i++)
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
                            catch (ConcurrencyException e) when (CanAvoidThrowingToMerger(e, i))
                            {
                                return 0;
                            }
                            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Id, cmd.Document.Size);
                            AddPutResult(putResult);
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
                                var deleteResults = Database.DocumentsStorage.DeleteDocumentsStartingWith(context, cmd.Id);

                                for (var j = 0; j < deleteResults.Count; j++)
                                {
                                    LastChangeVector = deleteResults[j].ChangeVector;
                                    ModifiedCollections?.Add(deleteResults[j].Collection.Name);
                                }

                                Reply.Add(new DynamicJsonValue
                                {
                                    [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                    [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.DELETE),
                                    ["Deleted"] = deleteResults.Count > 0
                                });
                            }
                            break;
                        case CommandType.AttachmentPUT:
                            var attachmentStream = AttachmentStreams.Dequeue();
                            var stream = attachmentStream.Stream;
                            _disposables.Add(stream);

                            var attachmentPutResult = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, cmd.Id, cmd.Name,
                                cmd.ContentType, attachmentStream.Hash, cmd.ChangeVector, stream, updateDocument: false);
                            LastChangeVector = attachmentPutResult.ChangeVector;

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            _documentsToUpdateAfterAttachmentChange.Add(cmd.Id);

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
                        case CommandType.Counters:
                            if (counterBatch.Documents == null)
                                counterBatch.Documents = new List<DocumentCountersOperation>();
                            counterBatch.Documents.Add(cmd.Counters);
                            break;
                    }
                }

                if (counterBatch.Documents != null)
                {
                    var cmd = new CountersHandler.ExecuteCounterBatchCommand(Database, counterBatch);
                    try
                    {
                        cmd.Execute(context);
                    }
                    catch (ConcurrencyException)
                    {
                        return 0;
                    }

                    Reply.Add(new DynamicJsonValue
                    {
                        [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.Counters),
                        ["CountersDetail"] = cmd.CountersDetail.ToJson(),
                    });
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
            
        }

        private class WaitForIndexItem
        {
            public Index Index;
            public AsyncManualResetEvent.FrozenAwaiter IndexBatchAwaiter;
            public AsyncWaitForIndexing WaitForIndexing;
        }
    }

}
