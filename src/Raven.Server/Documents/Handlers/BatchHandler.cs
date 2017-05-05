using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Exceptions;

namespace Raven.Server.Documents.Handlers
{
    public class BatchHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_docs", "POST")]
        public async Task BulkDocs()
        {
            DocumentsOperationContext ctx;
            using (ContextPool.AllocateOperationContext(out ctx))
            {
                var cmds = await BatchRequestParser.ParseAsync(ctx, RequestBodyStream(), Database.Patcher);
               
                var waitForIndexesTimeout = GetTimeSpanQueryString("waitForIndexesTimeout", required: false);

                using (var mergedCmd = new MergedBatchCommand
                {
                    Database = Database,
                    ParsedCommands = cmds,
                })
                {
                    if (waitForIndexesTimeout != null)
                        mergedCmd.ModifiedCollections = new HashSet<string>();

                    try
                    {
                        await Database.TxMerger.Enqueue(mergedCmd);
                    }
                    catch (ConcurrencyException)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.Conflict;
                        throw;
                    }

                    var waitForReplicasTimeout = GetTimeSpanQueryString("waitForReplicasTimeout", required: false);
                    if (waitForReplicasTimeout != null)
                    {
                        await WaitForReplicationAsync(waitForReplicasTimeout.Value, mergedCmd);
                    }

                    if (waitForIndexesTimeout != null)
                    {
                        await
                            WaitForIndexesAsync(waitForIndexesTimeout.Value, mergedCmd.LastEtag,
                                mergedCmd.ModifiedCollections);
                    }

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    using (var writer = new BlittableJsonTextWriter(ctx, ResponseBodyStream()))
                    {
                        ctx.Write(writer, new DynamicJsonValue
                        {
                            ["Results"] = mergedCmd.Reply
                        });
                    }
                }
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

            var waitForReplicationAsync = Database.ReplicationLoader.WaitForReplicationAsync(
                numberOfReplicasToWaitFor,
                waitForReplicasTimeout,
                mergedCmd.LastEtag);

            var replicatedPast = await waitForReplicationAsync;
            if (replicatedPast < numberOfReplicasToWaitFor && throwOnTimeoutInWaitForReplicas)
            {
                throw new TimeoutException(
                    $"Could not verify that etag {mergedCmd.LastEtag} was replicated to {numberOfReplicasToWaitFor} servers in {waitForReplicasTimeout}. So far, it only replicated to {replicatedPast}");
            }
        }

        private async Task WaitForIndexesAsync(TimeSpan timeout, long lastEtag, HashSet<string> modifiedCollections)
        {
            // waitForIndexesTimeout=timespan & waitForIndexThrow=false (default true)
            // waitForspecificIndex=specific index1 & waitForspecificIndex=specific index 2

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

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                while (true)
                {
                    var hadStaleIndexes = false;

                    using (context.OpenReadTransaction())
                    {
                        foreach (var waitForIndexItem in indexesToWait)
                        {
                            if (waitForIndexItem.Index.IsStale(context, lastEtag) == false)
                                continue;

                            hadStaleIndexes = true;

                            await waitForIndexItem.WaitForIndexing.WaitForIndexingAsync(waitForIndexItem.IndexBatchAwaiter);

                            if (waitForIndexItem.WaitForIndexing.TimeoutExceeded && throwOnTimeout)
                            {
                                throw new TimeoutException(
                                    $"After waiting for {sp.Elapsed}, could not verify that {indexesToCheck.Count} " +
                                    $"indexes has caught up with the changes as of etag: {lastEtag}");
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

            var specifiedIndexesQueryString = HttpContext.Request.Query["waitForSpecificIndexs"];

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
            public DocumentDatabase Database;
            public long LastEtag;

            public HashSet<string> ModifiedCollections;

            public override string ToString()
            {
                var sb = new StringBuilder($"{ParsedCommands.Count} commands").AppendLine();
                foreach (var cmd in ParsedCommands)
                {
                    sb.Append("\t")
                        .Append(cmd.Method)
                        .Append(" ")
                        .Append(cmd.Key)
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
                    switch (cmd.Method)
                    {
                        case BatchRequestParser.CommandType.PUT:
                            var putResult = Database.DocumentsStorage.Put(context, cmd.Key, cmd.Etag, cmd.Document);

                            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Key, cmd.Document.Size);

                            LastEtag = putResult.Etag;

                            ModifiedCollections?.Add(putResult.Collection.Name);

                            var changeVector = new DynamicJsonArray();
                            if (putResult.ChangeVector != null)
                            {
                                foreach (var entry in putResult.ChangeVector)
                                    changeVector.Add(entry.ToJson());
                            }
                            
                            // Make sure all the metadata fields are always been add
                            var putReply = new DynamicJsonValue
                            {
                                ["Method"] = "PUT",
                                [Constants.Documents.Metadata.Id] = putResult.Key,
                                [Constants.Documents.Metadata.Etag] = putResult.Etag,
                                [Constants.Documents.Metadata.Collection] = putResult.Collection.Name,
                                [Constants.Documents.Metadata.ChangeVector] = changeVector,
                                [Constants.Documents.Metadata.LastModified] = putResult.LastModified,
                            };

                            if (putResult.Flags != DocumentFlags.None)
                                putReply[Constants.Documents.Metadata.Flags] = putResult.Flags;

                            Reply.Add(putReply);
                            break;
                        case BatchRequestParser.CommandType.PATCH:

                            cmd.PatchCommand.Execute(context);

                            var patchResult = cmd.PatchCommand.PatchResult;
                            if (patchResult.ModifiedDocument != null)
                                context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(cmd.Key, patchResult.ModifiedDocument.Size);

                            if (patchResult.Etag != null)
                                LastEtag = patchResult.Etag.Value;

                            if (patchResult.Collection != null)
                                ModifiedCollections?.Add(patchResult.Collection);

                            Reply.Add(new DynamicJsonValue
                            {
                                ["Key"] = cmd.Key,
                                ["Etag"] = patchResult.Etag,
                                ["Method"] = "PATCH",
                                ["PatchStatus"] = patchResult.Status.ToString(),
                            });
                            break;
                        case BatchRequestParser.CommandType.DELETE:
                            
                            if (cmd.KeyPrefixed == false)
                            {
                                var deleted = Database.DocumentsStorage.Delete(context, cmd.Key, cmd.Etag);

                                if (deleted != null)
                                {
                                    LastEtag = deleted.Value.Etag;
                                    ModifiedCollections?.Add(deleted.Value.Collection.Name);
                                }

                                Reply.Add(new DynamicJsonValue
                                {
                                    ["Key"] = cmd.Key,
                                    ["Method"] = "DELETE",
                                    ["Deleted"] = deleted != null
                                });
                            }
                            else
                            {
                                var deleteResults = Database.DocumentsStorage.DeleteDocumentsStartingWith(context, cmd.Key);

                                for (var j = 0; j < deleteResults.Count; j++)
                                {
                                    LastEtag = deleteResults[j].Etag;
                                    ModifiedCollections?.Add(deleteResults[j].Collection.Name);
                                }

                                Reply.Add(new DynamicJsonValue
                                {
                                    ["Key"] = cmd.Key,
                                    ["Method"] = "DELETE",
                                    ["Deleted"] = deleteResults.Count > 0
                                });
                            }
                            
                            break;
                    }
                }
                return Reply.Count;
            }

            public void Dispose()
            {
                foreach (var cmd in ParsedCommands)
                {
                    cmd.Document?.Dispose();
                }
                BatchRequestParser.ReturnBuffer(ParsedCommands);
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