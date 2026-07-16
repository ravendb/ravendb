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

    internal static readonly TimeSpan RefreshDelay = TimeSpan.FromMinutes(10);

    public GenAiBatchPatchCommand(
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
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        var hashes = new Dictionary<string, (Document Doc, List<string> Hashes, bool Refresh)>();

        using (var statsScope = _scope.For(GenAiOperations.ApplyUpdateScript))
        {
            using (context.DocumentDatabase.Scripts.GetScriptRunner(_patchRequest, readOnly: false, out var runner))
            {
                foreach (var item in _items)
                {
                    statsScope.NumberOfContextObjects++;

                    if (item.ContextOutput.IsCached)
                        statsScope.TotalCachedContexts++;

                    ref var tuple = ref CollectionsMarshal.GetValueRefOrAddDefault(hashes, item.DocumentId, out var exists);
                    if (exists is false)
                    {
                        Document document = GetCurrentDocument(context, item.DocumentId);
                        tuple = (document, [], false);
                    }

                    if (item.UpdateHash == false)
                    {
                        tuple.Refresh = true;
                        continue;
                    }

                    if (tuple.Doc is null)
                        continue; // document was probably deleted while we talked to the model, skipping this

                    tuple.Hashes.Add(item.ContextOutput.AiHash);

                    if (item.ModelOutput is null)
                        continue;
                    
                    statsScope.TotalUpdates++;

                    var args = CreatePatchArgs(context, item);
                    try
                    {
                        var documentInstance = (BlittableObjectInstance)runner.Translate(context, tuple.Doc).AsObject();
                        using (var scriptResult = runner.Run(context, context, "execute", item.DocumentId, [documentInstance, args]))
                        using (var old = tuple.Doc.Data)
                        {
                            tuple.Doc.Data = scriptResult.TranslateToObject(context);
                        }
                    }
                    catch (Exception e)
                    {
                        // do not update metadata hash, log error, raise alert
                        tuple.Hashes.Remove(item.ContextOutput.AiHash);
                        var msg = $"Failed to apply update script for context in document '{item.DocumentId}'. " +
                                  $"Context was: {item.ContextOutput.Context}{Environment.NewLine}" +
                                  $"Error: {e}";

                        statsScope.UpdateFailures++;
                        _statistics.RecordItemLoadError(msg, item.DocumentId);
                        
                        if (_logger.IsWarnEnabled)
                            _logger.Warn(msg);
                    }
                }
            }

            var refreshAt = context.DocumentDatabase.Time.GetUtcNow().Add(RefreshDelay);
            foreach (var (id, (doc, allHashes, refresh)) in hashes)
                UpdateMetadata(id, doc?.Data, _taskIdentifier, allHashes, refresh ? refreshAt : null, context);

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

        return context.ReadObject(djv, item.DocumentId);
    }

    internal static BlittableJsonReaderObject UpdateMetadata(string id, BlittableJsonReaderObject doc, string taskIdentifier, List<string> allHashes, DateTime? refreshAt, DocumentsOperationContext context)
    {
        // no document, or nothing to write (no hashes and no @refresh) - nothing to persist
        if (doc == null || (allHashes.Count == 0 && refreshAt.HasValue == false))
            return null;

        var changed = false;
        if (doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
        {
            var newMetadata = new DynamicJsonValue();
            if (allHashes.Count > 0)
                newMetadata[Constants.Documents.Metadata.GenAiHashes] = new DynamicJsonValue { [taskIdentifier] = allHashes };
            if (refreshAt.HasValue)
                newMetadata[Constants.Documents.Metadata.Refresh] = refreshAt.Value;

            doc.Modifications = new DynamicJsonValue(doc)
            {
                [Constants.Documents.Metadata.Key] = newMetadata
            };
            changed = true;
        }
        else
        {
            changed = allHashes.Count > 0 && TryUpdateHashesIfNeeded(metadata, taskIdentifier, allHashes);

            if (refreshAt.HasValue && metadata.TryGet(Constants.Documents.Metadata.Refresh, out object _) == false)
            {
                metadata.Modifications ??= new DynamicJsonValue(metadata);
                metadata.Modifications[Constants.Documents.Metadata.Refresh] = refreshAt.Value;
                changed = true;
            }

            if (changed)
            {
                doc.Modifications = new DynamicJsonValue(doc)
                {
                    [Constants.Documents.Metadata.Key] = metadata
                };
            }
        }

        if (changed == false)
            return doc;

        doc = context.ReadObject(doc, id);

        context.DocumentDatabase.DocumentsStorage.Put(context, id, expectedChangeVector: null, doc);

        return doc;
    }

    private static bool TryUpdateHashesIfNeeded(BlittableJsonReaderObject metadata, string taskIdentifier, List<string> allHashes)
    {
        if (metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashes) == false)
        {
            metadata.Modifications ??= new DynamicJsonValue(metadata);
            metadata.Modifications[Constants.Documents.Metadata.GenAiHashes] = new DynamicJsonValue
            {
                [taskIdentifier] = allHashes
            };
            return true;
        }

        if (hashes.TryGet(taskIdentifier, out BlittableJsonReaderArray existingHashes) && existingHashes != null &&
            existingHashes.Length == allHashes.Count)
        {
            var needToUpdate = false;
            foreach (var hash in existingHashes)
            {
                if (allHashes.Contains(hash.ToString()))
                    continue;

                needToUpdate = true;
                break;
            }

            if (needToUpdate == false)
                return false;
        }

        hashes.Modifications = new DynamicJsonValue(hashes)
        {
            [taskIdentifier] = allHashes
        };
        metadata.Modifications ??= new DynamicJsonValue(metadata);
        metadata.Modifications[Constants.Documents.Metadata.GenAiHashes] = hashes;
        return true;
    }

    private Document GetCurrentDocument(DocumentsOperationContext context, string id)
    {
        var originalDocument = context.DocumentDatabase.DocumentsStorage.Get(context, id);

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

