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
using Raven.Server.Documents.Replication;
using System.Runtime.ExceptionServices;
using Raven.Client.Exceptions;

namespace Raven.Server.Documents.Handlers
{
    public class BatchHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_docs", "POST", AuthorizationStatus.ValidUser)]
        public async Task BulkDocs()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var command = new MergedBatchCommand { Database = Database })
            {
                var contentType = HttpContext.Request.ContentType;
                if (contentType == null ||
                    contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
                {
                    command.ParsedCommands = await BatchRequestParser.BuildCommandsAsync(context, RequestBodyStream(), Database, ServerStore);
                }
                else if (contentType.StartsWith("multipart/mixed", StringComparison.OrdinalIgnoreCase) || 
                    contentType.StartsWith("multipart/form-data", StringComparison.OrdinalIgnoreCase))
                {
                    await ParseMultipart(context, command);
                }
                else
                    ThrowNotSupportedType(contentType);

                var waitForIndexesTimeout = GetTimeSpanQueryString("waitForIndexesTimeout", required: false);
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
                    await WaitForReplicationAsync(waitForReplicasTimeout.Value, command);
                }

                if (waitForIndexesTimeout != null)
                {
                    await
                        WaitForIndexesAsync(waitForIndexesTimeout.Value, command.LastChangeVector, command.LastTombstoneEtag,
                            command.ModifiedCollections);
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Results"] = command.Reply
                    });
                }
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
                    command.ParsedCommands = await BatchRequestParser.BuildCommandsAsync(context, bodyStream, Database, ServerStore);
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

        private async Task WaitForReplicationAsync(TimeSpan waitForReplicasTimeout, MergedBatchCommand mergedCmd)
        {
            int numberOfReplicasToWaitFor;
            var numberOfReplicasStr = GetStringQueryString("numberOfReplicasToWaitFor", required: false) ?? "1";
            if (numberOfReplicasStr == "majority")
            {
                numberOfReplicasToWaitFor = Database.ReplicationLoader.GetSizeOfMajority();
            }
            else
            {
                if (int.TryParse(numberOfReplicasStr, out numberOfReplicasToWaitFor) == false)
                    ThrowInvalidInteger("numberOfReplicasToWaitFor", numberOfReplicasStr);
            }
            var throwOnTimeoutInWaitForReplicas = GetBoolValueQueryString("throwOnTimeoutInWaitForReplicas", required: false) ?? true;

            var replicatedPast = await Database.ReplicationLoader.WaitForReplicationAsync(
                numberOfReplicasToWaitFor,
                waitForReplicasTimeout,
                mergedCmd.LastChangeVector);

            if (replicatedPast < numberOfReplicasToWaitFor && throwOnTimeoutInWaitForReplicas)
            {
                var message = $"Could not verify that etag {mergedCmd.LastChangeVector} was replicated " +
                              $"to {numberOfReplicasToWaitFor} servers in {waitForReplicasTimeout}. " +
                              $"So far, it only replicated to {replicatedPast}";
                if (Logger.IsInfoEnabled)
                    Logger.Info(message);
                throw new TimeoutException(message);
            }
        }

        private async Task WaitForIndexesAsync(TimeSpan timeout, string lastChangeVector, long lastTombstoneEtag, HashSet<string> modifiedCollections)
        {
            // waitForIndexesTimeout=timespan & waitForIndexThrow=false (default true)
            // waitForSpecificIndex=specific index1 & waitForSpecificIndex=specific index 2

            if (modifiedCollections.Count == 0)
                return;

            var throwOnTimeout = GetBoolValueQueryString("waitForIndexThrow", required: false) ?? true;

            var indexesToWait = new List<WaitForIndexItem>();

            var indexesToCheck = GetImpactedIndexesToWaitForToBecomeNonStale(modifiedCollections);

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

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                while (true)
                {
                    var hadStaleIndexes = false;

                    using (context.OpenReadTransaction())
                    {
                        foreach (var waitForIndexItem in indexesToWait)
                        {
                            var lastEtag = lastChangeVector != null ? ChangeVectorParser.GetEtagByNode(lastChangeVector, ServerStore.NodeTag) : 0;

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

        private List<Index> GetImpactedIndexesToWaitForToBecomeNonStale(HashSet<string> modifiedCollections)
        {
            var indexesToCheck = new List<Index>();

            var specifiedIndexesQueryString = HttpContext.Request.Query["waitForSpecificIndex"];

            if (specifiedIndexesQueryString.Count > 0)
            {
                var specificIndexes = specifiedIndexesQueryString.ToHashSet();
                foreach (var index in Database.IndexStore.GetIndexes())
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
                foreach (var index in Database.IndexStore.GetIndexes())
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

        public class MergedBatchCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            public DynamicJsonArray Reply;
            public ArraySegment<BatchRequestParser.CommandData> ParsedCommands;
            public Queue<AttachmentStream> AttachmentStreams;
            public StreamsTempFile AttachmentStreamsTempFile;
            public DocumentDatabase Database;
            public string LastChangeVector;
            public long LastTombstoneEtag;
            private HashSet<string> _documentsToUpdateAfterAttachmentChange;
            public HashSet<string> ModifiedCollections;
            private readonly List<IDisposable> _disposables = new List<IDisposable>();
            public ExceptionDispatchInfo ExceptionDispatchInfo;

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
                Reply = new DynamicJsonArray();
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
                                if (deleted != null)
                                {
                                    LastTombstoneEtag = deleted.Value.Etag;
                                    ModifiedCollections?.Add(deleted.Value.Collection.Name);
                                }

                                Reply.Add(new DynamicJsonValue
                                {
                                    [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                    [nameof(BatchRequestParser.CommandData.Type)] = nameof(CommandType.DELETE),
                                    ["Deleted"] = deleted != null
                                });
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
        }

        private class WaitForIndexItem
        {
            public Index Index;
            public AsyncManualResetEvent.FrozenAwaiter IndexBatchAwaiter;
            public AsyncWaitForIndexing WaitForIndexing;
        }
    }
}
