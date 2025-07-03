using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Jint;
using Raven.Client;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

internal sealed class GenAiBatchPatchCommand : DocumentMergedTransactionCommand
{
    private readonly List<GenAiResultItem> _items;
    private readonly PatchRequest _patchRequest;
    private readonly string _taskIdentifier;
    private readonly RavenLogger _logger;
    private readonly EtlProcessStatistics _statistics;
    private readonly GenAiStatsScope _scope;
    private readonly DocumentDatabase _database;

    public GenAiBatchPatchCommand(DocumentsOperationContext context,
        List<GenAiResultItem> items,
        PatchRequest patchRequest,
        string taskIdentifier,
        RavenLogger logger,
        EtlProcessStatistics statistics, 
        GenAiStatsScope scope)
    {
        _items = items ?? throw new ArgumentException(nameof(items));
        _patchRequest = patchRequest ?? throw new ArgumentException(nameof(patchRequest));
        _logger = logger ?? throw new ArgumentException(nameof(logger));
        _statistics = statistics ?? throw new ArgumentException(nameof(statistics));
        _scope = scope;

        if (string.IsNullOrEmpty(taskIdentifier))
            throw new ArgumentException(nameof(taskIdentifier));
        _taskIdentifier = taskIdentifier;

        if (context == null)
            throw new ArgumentNullException(nameof(context));
        _database = context.DocumentDatabase;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        var hashes = new Dictionary<string, (Document Doc, List<string> Hashes)>();

        using (var statsScope = _scope.For(GenAiOperations.ApplyUpdateScript))
        {
            using (_database.Scripts.GetScriptRunner(_patchRequest, readOnly: false, out var runner))
            {
                foreach (var item in _items)
                {
                    statsScope.NumberOfContextObjects++;

                    if (item.ContextOutput.IsCached)
                        statsScope.TotalCachedContexts++;

                    if (item.UpdateHash == false)
                        continue;
                    
                    ref var tuple = ref CollectionsMarshal.GetValueRefOrAddDefault(hashes, item.DocId, out var exists);
                    if (exists is false)
                    {
                        Document document = GetCurrentDocument(context, item.DocId);
                        if (document is null)
                            continue; // document was probably deleted while we talked to the model, skipping this

                        tuple = (document, []);
                    }

                    tuple.Hashes.Add(item.ContextOutput.AiHash);

                    if (item.ModelOutput is null)
                        continue;
                    
                    statsScope.TotalUpdates++;

                    var args = CreatePatchArgs(context, item);
                    try
                    {
                        var documentInstance = (BlittableObjectInstance)runner.Translate(context, tuple.Doc).AsObject();
                        using (var scriptResult = runner.Run(context, context, "execute", item.DocId, [documentInstance, args]))
                        using (var old = tuple.Doc.Data)
                        {
                            tuple.Doc.Data = scriptResult.TranslateToObject(context);
                        }
                    }
                    catch (Exception e)
                    {
                        // do not update metadata hash, log error, raise alert
                        tuple.Hashes.Remove(item.ContextOutput.AiHash);
                        var msg = $"Failed to apply update script for context in document '{item.DocId}'. " +
                                  $"Context was: {item.ContextOutput.Context}{Environment.NewLine}" +
                                  $"Error: {e}";

                        statsScope.UpdateFailures++;
                        _statistics.RecordPartialLoadError(msg, item.DocId);
                        
                        if (_logger.IsWarnEnabled)
                            _logger.Warn(msg);
                    }
                }
            }

            // update metadata for each doc in same transaction
            foreach (var (id, (doc, allHashes)) in hashes)
            {
                // this indicates that there was an error in the update script
                // and that we should not update this document
                if (allHashes.Count is 0)
                    continue;

                UpdateHashesInMetadata(id, doc.Data, _taskIdentifier, allHashes, context);
            }

            return statsScope.TotalUpdates;
        }
    }

    private static BlittableJsonReaderObject CreatePatchArgs(DocumentsOperationContext context, GenAiResultItem item)
    {
        var djv = new DynamicJsonValue
        {
            ["output"] = item.ModelOutput.Output,
            ["input"] = item.ContextOutput.Context
        };

        return context.ReadObject(djv, item.DocId);
    }

    internal static BlittableJsonReaderObject UpdateHashesInMetadata(string id, BlittableJsonReaderObject doc, string taskIdentifier, List<string> allHashes, DocumentsOperationContext context)
    {
        if (doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
        {
            // no metadata at all (shouldn't happen)

            doc.Modifications = new DynamicJsonValue(doc)
            {
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.GenAiHashes] = new DynamicJsonValue
                    {
                        [taskIdentifier] = allHashes
                    }
                }
            };
        }

        else if (metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashes) == false)
        {
            // no hashes section

            metadata.Modifications = new DynamicJsonValue(metadata)
            {
                [Constants.Documents.Metadata.GenAiHashes] = new DynamicJsonValue
                {
                    [taskIdentifier] = allHashes
                }
            };
            doc.Modifications = new DynamicJsonValue(doc)
            {
                [Constants.Documents.Metadata.Key] = metadata
            };
        }

        else
        {
            // we already have the hashes section, need to modify it

            if (hashes.TryGet(taskIdentifier, out BlittableJsonReaderArray existingHashes) && existingHashes != null && 
                existingHashes.Length == allHashes.Count)
            {
                bool needToUpdate = false;

                foreach (var hash in existingHashes)
                {
                    if (allHashes.Contains(hash.ToString())) 
                        continue;

                    // we have a new hash that is not in the existing hashes
                    needToUpdate = true;
                    break;
                }

                if (needToUpdate == false)
                    return doc; // we already have the hashes for this task, no need to update
            }

            hashes.Modifications = new DynamicJsonValue(hashes)
            {
                [taskIdentifier] = allHashes
            };

            metadata.Modifications = new DynamicJsonValue(metadata)
            {
                [Constants.Documents.Metadata.GenAiHashes] = hashes
            };

            doc.Modifications = new DynamicJsonValue(doc)
            {
                [Constants.Documents.Metadata.Key] = metadata
            };
        }

        using (var old = doc)
        {
            doc = context.ReadObject(old, id);
        }

        context.DocumentDatabase.DocumentsStorage.Put(context, id, expectedChangeVector: null, doc);

        return doc;
    }

    private Document GetCurrentDocument(DocumentsOperationContext context, string id)
    {
        var originalDocument = _database.DocumentsStorage.Get(context, id);

        if (originalDocument != null)
        {
            using (var oldData = originalDocument.Data)
            {
                // we clone it, to keep it safe from defrag due to the patch modifications
                originalDocument.Data = originalDocument.Data?.CloneOnTheSameContext();
            }
        }

        return originalDocument;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
    {
        throw new NotSupportedException($"Replay not supported for {nameof(GenAiBatchPatchCommand)}");
    }
}

