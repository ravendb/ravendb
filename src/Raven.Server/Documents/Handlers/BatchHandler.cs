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
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Exceptions;
using Raven.Server.Documents.Replication;

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
                else if (contentType.StartsWith("multipart/mixed", StringComparison.OrdinalIgnoreCase))
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
                        WaitForIndexesAsync(waitForIndexesTimeout.Value, command.LastChangeVector,
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
            var throwOnTimeoutInWaitForReplicas = GetBoolValueQueryString("throwOnTimeoutInWaitForReplicas") ?? true;

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

        private async Task WaitForIndexesAsync(TimeSpan timeout, string lastChangeVector, HashSet<string> modifiedCollections)
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
                            if (waitForIndexItem.Index.IsStale(context, ChangeVectorParser.GetEtagByNode(lastChangeVector,ServerStore.NodeTag)) == false)
                                continue;

                            hadStaleIndexes = true;

                            await waitForIndexItem.WaitForIndexing.WaitForIndexingAsync(waitForIndexItem.IndexBatchAwaiter);

                            if (waitForIndexItem.WaitForIndexing.TimeoutExceeded && throwOnTimeout)
                            {
                                throw new TimeoutException(
                                    $"After waiting for {sp.Elapsed}, could not verify that {indexesToCheck.Count} " +
                                    $"indexes has caught up with the changes as of etag: {lastChangeVector}");
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
                    if (index.Collections.Count == 0 || index.Collections.Overlaps(modifiedCollections))
                        indexesToCheck.Add(index);
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
            private HashSet<string> _documentsToUpdateAfterAttachmentChange;
            public HashSet<string> ModifiedCollections;

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

            public override int Execute(DocumentsOperationContext context)
            {
                Reply = new DynamicJsonArray();
                for (int i = ParsedCommands.Offset; i < ParsedCommands.Count; i++)
                {
                    var cmd = ParsedCommands.Array[ParsedCommands.Offset + i];
                    switch (cmd.Type)
                    {
                        case CommandType.PUT:
                        {
                            var putResult = Database.DocumentsStorage.Put(context, cmd.Id, cmd.ChangeVector, cmd.Document);
                            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Id, cmd.Document.Size);
                            LastChangeVector = putResult.ChangeVector;
                            ModifiedCollections?.Add(putResult.Collection.Name);

                            // Make sure all the metadata fields are always been add
                            var putReply = new DynamicJsonValue
                            {
                                ["Type"] = CommandType.PUT.ToString(),
                                [Constants.Documents.Metadata.Id] = putResult.Id,
                                [Constants.Documents.Metadata.Collection] = putResult.Collection.Name,
                                [Constants.Documents.Metadata.ChangeVector] = putResult.ChangeVector,
                                [Constants.Documents.Metadata.LastModified] = putResult.LastModified
                            };

                            if (putResult.Flags != DocumentFlags.None)
                                putReply[Constants.Documents.Metadata.Flags] = putResult.Flags;

                            Reply.Add(putReply);
                        }
                            break;
                        case CommandType.PATCH:
                            cmd.PatchCommand.Execute(context);

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
                                [nameof(BatchRequestParser.CommandData.Type)] = CommandType.PATCH.ToString(),
                                ["PatchStatus"] = patchResult.Status.ToString()
                            });
                            break;
                        case CommandType.DELETE:
                            if (cmd.IdPrefixed == false)
                            {
                                var deleted = Database.DocumentsStorage.Delete(context, cmd.Id, cmd.ChangeVector);

                                if (deleted != null)
                                {
                                    LastChangeVector = deleted.Value.ChangeVector;
                                    ModifiedCollections?.Add(deleted.Value.Collection.Name);
                                }

                                Reply.Add(new DynamicJsonValue
                                {
                                    [nameof(BatchRequestParser.CommandData.Id)] = cmd.Id,
                                    [nameof(BatchRequestParser.CommandData.Type)] = CommandType.DELETE.ToString(),
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
                                    [nameof(BatchRequestParser.CommandData.Type)] = CommandType.DELETE.ToString(),
                                    ["Deleted"] = deleteResults.Count > 0
                                });
                            }
                            break;
                        case CommandType.AttachmentPUT:
                            var attachmentStream = AttachmentStreams.Dequeue();
                            using (var stream = attachmentStream.Stream)
                            {
                                var attachmentPutResult = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, cmd.Id, cmd.Name,
                                    cmd.ContentType, attachmentStream.Hash, cmd.ChangeVector, stream, updateDocument: false);
                                LastChangeVector = attachmentPutResult.ChangeVector;

                                if (_documentsToUpdateAfterAttachmentChange == null)
                                    _documentsToUpdateAfterAttachmentChange = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                _documentsToUpdateAfterAttachmentChange.Add(cmd.Id);

                                Reply.Add(new DynamicJsonValue
                                {
                                    [nameof(BatchRequestParser.CommandData.Id)] = attachmentPutResult.DocumentId,
                                    [nameof(BatchRequestParser.CommandData.Type)] = CommandType.AttachmentPUT.ToString(),
                                    [nameof(BatchRequestParser.CommandData.Name)] = attachmentPutResult.Name,
                                    [nameof(BatchRequestParser.CommandData.ChangeVector)] = attachmentPutResult.ChangeVector,
                                    [nameof(AttachmentDetails.Hash)] = attachmentPutResult.Hash,
                                    [nameof(BatchRequestParser.CommandData.ContentType)] = attachmentPutResult.ContentType,
                                    [nameof(AttachmentDetails.Size)] = attachmentPutResult.Size
                                });
                            }

                            break;
                        case CommandType.AttachmentDELETE:
                            Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, cmd.Id, cmd.Name, cmd.ChangeVector, updateDocument: false);

                            if (_documentsToUpdateAfterAttachmentChange == null)
                                _documentsToUpdateAfterAttachmentChange = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            _documentsToUpdateAfterAttachmentChange.Add(cmd.Id);

                            Reply.Add(new DynamicJsonValue
                            {
                                ["Type"] = CommandType.AttachmentPUT.ToString(),
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

                foreach (var cmd in ParsedCommands)
                {
                    cmd.Document?.Dispose();
                    if(cmd.PatchCommand!= null)
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
