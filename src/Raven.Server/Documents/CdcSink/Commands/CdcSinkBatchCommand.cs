using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Jint;
using Jint.Native;
using Raven.Client;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.CdcSink.Commands;

public sealed class CdcSinkBatchCommand : DocumentMergedTransactionCommand
{
    /// <summary>
    /// A single patch to run on the document. Old is the previous item's blittable (embedded / root)
    /// (for array: the matched item, for value: the existing object) — null for inserts. Previous value for deletes and updates.
    /// Provides $old in scripts for delta computations: this.Total += $row.Amount - ($old?.Amount || 0)
    /// </summary>
    private readonly record struct PatchEntry(string TableName, string[] ColumnNames, object[] Values, string PatchScript, BlittableJsonReaderObject Old);

    private readonly DocumentDatabase _database;
    private readonly List<CdcSinkDocumentOp> _ops;
    private readonly string _configurationName;
    private readonly string _lastLsn;
    private readonly Dictionary<string, CdcSinkTableLoadState> _tableLoadUpdates;
    private readonly PatchRequest _patchRequest;
    private readonly CdcSinkStatsScope _statsScope;
    private readonly CdcSinkProcessStatistics _statistics;
    private readonly RavenLogger _logger;
    private StringBuilder _sb;

    public int ProcessedSuccessfully { get; private set; }

    /// <summary>
    /// Raised for each document that fails during batch processing.
    /// The event provides the document ID and the exception.
    /// </summary>
    internal event Action<string, Exception> DocumentProcessingError;

    public CdcSinkBatchCommand(
        DocumentDatabase database,
        List<CdcSinkDocumentOp> ops,
        string configurationName,
        string lastLsn,
        Dictionary<string, CdcSinkTableLoadState> tableLoadUpdates,
        PatchRequest patchRequest,
        CdcSinkStatsScope statsScope,
        CdcSinkProcessStatistics statistics,
        RavenLogger logger)
    {
        _database = database;
        _ops = ops;
        _configurationName = configurationName;
        _lastLsn = lastLsn;
        _tableLoadUpdates = tableLoadUpdates;
        _patchRequest = patchRequest;
        _statsScope = statsScope;
        _statistics = statistics;
        _logger = logger;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        var groups = GroupByDocumentId(_ops);
        int batchErrors = 0;

        foreach (var (documentId, ops) in groups)
        {
            try
            {
                ProcessDocumentGroup(context, documentId, ops);
                ProcessedSuccessfully += ops.Count;
                _statistics?.ConsumeSuccess(ops.Count);
                _statsScope?.RecordProcessedMessage();
            }
            catch (Exception e)
            {
                batchErrors++;

                if (_logger?.IsErrorEnabled == true)
                    _logger.Error($"Failed to process CDC operations for document '{documentId}'.", e);

                DocumentProcessingError?.Invoke(documentId, e);
                _statsScope?.RecordScriptProcessingError();

                // RecordPartialConsumeError tracks cumulative error/success counts and throws
                // InvalidOperationException when the error ratio is too high (>=100 errors AND
                // errors > successes), preventing LSN advancement for a poisoned stream.
                _statistics?.RecordPartialConsumeError(e.ToString(), documentId);
            }
        }

        if (batchErrors == 0 || ProcessedSuccessfully > 0)
        {
            // Advance LSN only when the entire batch succeeded, or when some items
            // were processed successfully and the error ratio is still tolerable
            // (if it weren't tolerable, RecordPartialConsumeError would have thrown above).
            UpdateState(context);
        }

        return _ops.Count;
    }

    /// <summary>
    /// Process a sequence of operations for a single document in order.
    /// A sequence like [put, delete, embed, embed, put, delete, put, embed] is replayed sequentially:
    ///  - Put: apply (Object.assign) onto the current state
    ///  - Delete: null the current state and clear pending embeds
    ///  - EmbeddedModify: add to pending embeds list
    /// At the end, the surviving state (if any) gets embedded ops applied, then patches, then saved.
    /// </summary>
    private void ProcessDocumentGroup(DocumentsOperationContext context, string documentId, List<CdcSinkDocumentOp> ops)
    {
        BlittableJsonReaderObject currentDoc = null, deletedDoc = null;
        List<CdcSinkDocumentOp> pendingEmbeds = null;

        List<PatchEntry> patches = null;
        CdcSinkDocumentOp lastPutOp = null;
        CdcSinkDocumentOp lastDeleteOp = null;
        string collectionName = null;

        var existing = _database.DocumentsStorage.Get(context, documentId);
        if (existing != null)
            currentDoc = existing.Data;

        for (int i = 0; i < ops.Count; i++)
        {
            var op = ops[i];
            collectionName ??= op.Processor.CollectionName;

            switch (op.Type)
            {
                case CdcSinkDocumentOpType.Put:
                    if (op.Processor.RootConfig.Patch != null)
                    {
                        patches ??= [];
                        patches.Add(new PatchEntry(op.Processor.Key, op.Processor.SourceColumnNames, op.RawValues, op.Processor.RootConfig.Patch, currentDoc));
                    }

                    if (currentDoc != null)
                    {
                        // Merge new values onto existing document — preserves properties
                        // that aren't in the CDC mapping (e.g., fields set directly in RavenDB)
                        currentDoc.Modifications ??= new DynamicJsonValue(currentDoc);
                        foreach (var (name, value) in op.MappedData.Properties)
                            currentDoc.Modifications[name] = value;
                        currentDoc = context.ReadObject(currentDoc, documentId, BlittableJsonDocumentBuilder.UsageMode.None);
                    }
                    else
                    {
                        currentDoc = context.ReadObject(op.MappedData, documentId, BlittableJsonDocumentBuilder.UsageMode.None);
                    }

                    lastPutOp = op;
                    lastDeleteOp = null;
                    deletedDoc = null;

                    break;
                case CdcSinkDocumentOpType.Delete:
                    var ignoreThisDelete = op.Processor.RootConfig.OnDelete?.IgnoreDeletes == true;

                    // Flush pending embeds so their patches run
                    currentDoc = FlushPendingEmbeds(context, documentId, currentDoc, pendingEmbeds, ref patches);

                    // Add the OnDelete patch
                    var deleteScript = op.Processor.RootConfig.OnDelete?.Patch;
                    if (deleteScript != null)
                    {
                        patches ??= [];
                        patches.Add(new PatchEntry(op.Processor.KeyOnDelete, op.Processor.SourceColumnNames, op.RawValues, deleteScript, currentDoc));
                    }

                    if (ignoreThisDelete)
                    {
                        // IgnoreDeletes: the document survives unless the patch explicitly
                        // calls del(). Run patches and keep the result as currentDoc so
                        // modifications (e.g., this.Archived = true) persist into subsequent
                        // Puts or the final save. When RunPatches returns null, it means the
                        // script called del() or put() — respect that decision by setting
                        // currentDoc to null so we don't resurrect the document with a Put.
                        if (patches is { Count: > 0 } && currentDoc != null)
                        {
                            var result = RunPatches(context, documentId, currentDoc, patches);
                            if (result != null)
                            {
                                currentDoc = result;
                            }
                            else
                            {
                                // Script called del()/put() — the document was already
                                // removed or replaced by the script engine. Clear state
                                // so we don't issue a spurious Put at the end.
                                currentDoc = null;
                                lastPutOp = null;
                                lastDeleteOp = op;
                                deletedDoc = null;
                            }
                        }
                        patches = null;
                        pendingEmbeds = null;
                    }
                    else
                    {
                        // Real delete: flush patches for side effects (e.g., audit put() calls)
                        // but discard modifications to 'this' — a subsequent Put starts fresh.
                        if (patches is { Count: > 0 } && currentDoc != null)
                        {
                            var patchedDoc = RunPatches(context, documentId, currentDoc, patches);
                            deletedDoc = patchedDoc ?? currentDoc;
                        }
                        else
                        {
                            deletedDoc = currentDoc;
                        }

                        patches = null;
                        pendingEmbeds = null;
                        currentDoc = null;
                        lastPutOp = null;
                        lastDeleteOp = op;
                    }

                    break;
                case CdcSinkDocumentOpType.EmbeddedModify:
                    deletedDoc = null;
                    lastDeleteOp = null;

                    pendingEmbeds ??= [];
                    pendingEmbeds.Add(op);
                    // Embedded patches are collected after ApplyEmbeddedOperation, not here,
                    // so we have access to $old (the previous embedded item data for updates).
                    break;
            }
        }

        if (deletedDoc != null)
        {
            // Patches have already been flushed inline during the Delete op above.
            var ignoreDeletes = lastDeleteOp.Processor.RootConfig.OnDelete?.IgnoreDeletes == true;

            if (ignoreDeletes)
            {
                // Archive pattern: save the (potentially patched) document, skip delete
                _database.DocumentsStorage.Put(context, documentId, expectedChangeVector: null, deletedDoc);
                return;
            }

            _database.DocumentsStorage.Delete(context, documentId, null);
            return;
        }
        // If document is null and we have pending embeds, create a stub
        if (currentDoc == null && pendingEmbeds is { Count: > 0 })
        {
            var stub = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = collectionName,
                }
            };
            currentDoc = context.ReadObject(stub, documentId, BlittableJsonDocumentBuilder.UsageMode.None);
        }

        currentDoc = FlushPendingEmbeds(context, documentId, currentDoc, pendingEmbeds, ref patches, includeOnDelete: true);

        if (patches is { Count: > 0 })
        {
            currentDoc = RunPatches(context, documentId, currentDoc, patches);
        }

        if (currentDoc is null)
        {
            // RunPatches returned null — the script called del() or put().
            // If del(): document is gone, skip attachments.
            // If put(): script replaced the document; attachments should still be stored
            //   since the script may have called put(id(this), modified) to save a
            //   transformed version but still expects binary columns as attachments.
            if (_database.DocumentsStorage.Get(context, documentId) == null)
                return; // document was deleted, nothing to attach to
        }
        else
        {
            _database.DocumentsStorage.Put(context, documentId, expectedChangeVector: null, currentDoc);
        }

        if (lastPutOp != null) // Store attachments from root table binary columns
            StoreAttachments(context, documentId, lastPutOp.RawValues, lastPutOp.Processor, prefix: null);

        // Handle attachments from embedded table binary columns.
        // Attachment name includes the embedded path and PK values to distinguish
        // attachments from different embedded rows (e.g., "Lines/42/photo").
        foreach (var embOp in pendingEmbeds ?? [])
        {
            var attachmentColumns = embOp.Processor.AttachmentColumns;
            if (attachmentColumns.Count == 0)
                continue;

            var prefix = BuildEmbeddedAttachmentPrefix(embOp.Processor, embOp.RawValues);

            if (embOp.Operation == CdcSinkOperation.Upsert)
            {
                StoreAttachments(context, documentId, embOp.RawValues, embOp.Processor, prefix);
            }
            else
            {
                DeleteAttachments(context, documentId, attachmentColumns, prefix);
            }
        }
    }

    private void StoreAttachments(
        DocumentsOperationContext context, string documentId,
        object[] values, CdcSinkTableProcessor processor, string prefix)
    {
        var attachmentColumns = processor.AttachmentColumns;
        var indices = processor.AttachmentColumnIndices;
        for (int i = 0; i < attachmentColumns.Count; i++)
        {
            var col = attachmentColumns[i];
            var idx = indices[i];
            var value = values[idx];
            if (value is null or DBNull)
                continue;

            var name = prefix != null ? prefix + col.Name : col.Name;

            switch (value)
            {
                case byte[] b:
                    StoreAttachmentFromSpan(context, documentId, name, "application/octet-stream", b);
                    break;
                case string s:
                    StoreStringAttachment(context, documentId, name, s);
                    break;
                case float[] floats:
                    StoreAttachmentFromSpan(context, documentId, name, "application/octet-stream", MemoryMarshal.AsBytes(floats.AsSpan()));
                    break;
                case double[] doubles:
                    StoreAttachmentFromSpan(context, documentId, name, "application/octet-stream", MemoryMarshal.AsBytes(doubles.AsSpan()));
                    break;
                default:
                    throw new NotSupportedException($"Unsupported attachment type '{value.GetType().FullName}' for column '{col.Column}' on document '{documentId}'.");
            }
        }
    }

    private unsafe void StoreAttachmentFromSpan(
        DocumentsOperationContext context, string documentId,
        string name, string contentType, ReadOnlySpan<byte> data)
    {
        var hash = AttachmentsStorageHelper.CalculateHash(data);

        if (data.Length == 0) // empty buffer means empty attachment
        {
            using var stream = new MemoryStream([], 0, 0, writable: false);
            _database.DocumentsStorage.AttachmentsStorage.PutAttachment(
                context, documentId, name, contentType,
                hash, 0, remoteParams: null, stream: stream);
            return;
        }

        fixed (byte* ptr = data)
        {
            using var stream = new UnmanagedMemoryStream(ptr, data.Length);
            _database.DocumentsStorage.AttachmentsStorage.PutAttachment(
                context, documentId, name, contentType,
                hash, data.Length, remoteParams: null, stream: stream);
        }
    }

    private void StoreStringAttachment(
        DocumentsOperationContext context, string documentId,
        string name, string s)
    {
        var byteCount = Encoding.UTF8.GetByteCount(s);
        var rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            var written = Encoding.UTF8.GetBytes(s, 0, s.Length, rented, 0);
            StoreAttachmentFromSpan(context, documentId, name, "text/plain; charset=utf-8", new ReadOnlySpan<byte>(rented, 0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private void DeleteAttachments(
        DocumentsOperationContext context, string documentId,
        List<CdcColumnMapping> attachmentColumns, string prefix)
    {
        for (int i = 0; i < attachmentColumns.Count; i++)
        {
            var col = attachmentColumns[i];
            var name = prefix != null ? prefix + col.Name : col.Name;
            _database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(
                context, documentId, name, expectedChangeVector: null, collectionName: out _, updateDocument: true);
        }
    }

    private string BuildEmbeddedAttachmentPrefix(CdcSinkTableProcessor processor, object[] values)
    {
        _sb ??= new StringBuilder();
        _sb.Clear();
        _sb.Append(processor.EmbeddedConfig.PropertyName).Append('/');
        var pkIndices = processor.PrimaryKeyIndices;
        for (int j = 0; j < pkIndices.Length; j++)
        {
            if (j > 0)
                _sb.Append('/');
            var val = values[pkIndices[j]];
            _sb.Append(val is null or DBNull ? "null" : val.ToString());
        }
        _sb.Append('/');
        return _sb.ToString();
    }

    /// <summary>
    /// Applies all pending embedded operations to the document, collecting patch entries
    /// with $old for each operation that has a patch script.
    ///
    /// This method is called twice per document group:
    ///   1. Before a root-level delete (includeOnDelete = false, the default) — we flush
    ///      pending embed upserts/deletes but do NOT collect OnDelete patches yet, because
    ///      the root delete handler manages those separately.
    ///   2. At the end of the group (includeOnDelete = true) — we apply the remaining
    ///      embedded operations and DO collect OnDelete.Patch entries for embedded deletes,
    ///      because this is the final pass where all patches are gathered before execution.
    /// </summary>
    private BlittableJsonReaderObject FlushPendingEmbeds(
        DocumentsOperationContext context, string documentId,
        BlittableJsonReaderObject currentDoc, List<CdcSinkDocumentOp> pendingEmbeds,
        ref List<PatchEntry> patches, bool includeOnDelete = false)
    {
        if (pendingEmbeds == null || pendingEmbeds.Count == 0)
            return currentDoc;

        // Fast path: single op, no batching overhead needed
        if (pendingEmbeds.Count == 1)
            return FlushSingleEmbed(context, documentId, currentDoc, pendingEmbeds[0], ref patches, includeOnDelete);

        // Deferred materialization: accumulate all Modifications on the same root blittable,
        // then materialize once at the end. This avoids k full-document ReadObject calls
        // (O(k * docSize)) and replaces them with a single one (O(docSize)).
        //
        // For array-type embedded properties, we maintain a per-property DynamicJsonArray
        // working copy so that subsequent ops on the same array see the accumulated state
        // (inserts, deletes, updates from earlier ops in the same batch).
        currentDoc.Modifications ??= new DynamicJsonValue(currentDoc);

        // Cache of working arrays per property, keyed by processor Key (which includes schema.table).
        // Avoids re-reading the array from the blittable for each op targeting the same property.
        Dictionary<string, DynamicJsonArray> arrayStates = null;

        bool modified = false;
        foreach (var embedOp in pendingEmbeds)
        {
            var isDelete = embedOp.Operation == CdcSinkOperation.Delete;
            var embeddedConfig = embedOp.Processor.EmbeddedConfig;

            // IgnoreDeletes: the item stays, peek $old without mutating
            if (isDelete && includeOnDelete && embeddedConfig.OnDelete?.IgnoreDeletes == true)
            {
                if (embeddedConfig.OnDelete.Patch != null)
                {
                    var existingItem = FindExistingEmbeddedItem(context, currentDoc, embedOp);
                    patches ??= [];
                    patches.Add(new PatchEntry(embedOp.Processor.KeyOnDelete, embedOp.Processor.SourceColumnNames, embedOp.RawValues, embeddedConfig.OnDelete.Patch, existingItem));
                }
                continue;
            }

            bool needsOld = (isDelete && includeOnDelete && embeddedConfig.OnDelete?.Patch != null) ||
                            (!isDelete && embeddedConfig.Patch != null);

            var (target, targetBlittable) = NavigateToEmbeddedParent(
                currentDoc, currentDoc.Modifications, embedOp.Processor.PathFromRoot, embedOp);

            BlittableJsonReaderObject old;

            switch (embeddedConfig.Type)
            {
                case CdcSinkRelationType.Array:
                    old = ApplyArrayOperationDeferred(context, targetBlittable, target,
                        embeddedConfig, embedOp, needsOld, ref arrayStates);
                    break;
                case CdcSinkRelationType.Map:
                    old = ApplyMapOperation(context, targetBlittable, target, embeddedConfig, embedOp, needsOld);
                    break;
                case CdcSinkRelationType.Value:
                    old = ApplyValueOperation(context, targetBlittable, target, embeddedConfig, embedOp, needsOld);
                    break;
                default:
                    old = null;
                    break;
            }

            modified = true;

            if (isDelete && includeOnDelete && embeddedConfig.OnDelete?.Patch != null)
            {
                patches ??= [];
                patches.Add(new PatchEntry(embedOp.Processor.KeyOnDelete, embedOp.Processor.SourceColumnNames, embedOp.RawValues, embeddedConfig.OnDelete.Patch, old));
            }
            else if (!isDelete && embeddedConfig.Patch != null)
            {
                patches ??= [];
                patches.Add(new PatchEntry(embedOp.Processor.Key, embedOp.Processor.SourceColumnNames, embedOp.RawValues, embeddedConfig.Patch, old));
            }
        }

        // Single materialization for all embedded ops
        if (modified)
            currentDoc = context.ReadObject(currentDoc, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

        return currentDoc;
    }

    /// <summary>
    /// Fast path for a single embedded op — no deferred materialization needed.
    /// </summary>
    private BlittableJsonReaderObject FlushSingleEmbed(
        DocumentsOperationContext context, string documentId,
        BlittableJsonReaderObject currentDoc, CdcSinkDocumentOp embedOp,
        ref List<PatchEntry> patches, bool includeOnDelete)
    {
        var isDelete = embedOp.Operation == CdcSinkOperation.Delete;
        var embeddedConfig = embedOp.Processor.EmbeddedConfig;

        if (isDelete && includeOnDelete && embeddedConfig.OnDelete?.IgnoreDeletes == true)
        {
            if (embeddedConfig.OnDelete.Patch != null)
            {
                var existingItem = FindExistingEmbeddedItem(context, currentDoc, embedOp);
                patches ??= [];
                patches.Add(new PatchEntry(embedOp.Processor.KeyOnDelete, embedOp.Processor.SourceColumnNames, embedOp.RawValues, embeddedConfig.OnDelete.Patch, existingItem));
            }
            return currentDoc;
        }

        bool needsOld = (isDelete && includeOnDelete && embeddedConfig.OnDelete?.Patch != null) ||
                        (!isDelete && embeddedConfig.Patch != null);

        var (updatedDoc, old) = ApplyEmbeddedOperation(context, documentId, currentDoc, embedOp, needsOld);

        if (isDelete && includeOnDelete && embeddedConfig.OnDelete?.Patch != null)
        {
            patches ??= [];
            patches.Add(new PatchEntry(embedOp.Processor.KeyOnDelete, embedOp.Processor.SourceColumnNames, embedOp.RawValues, embeddedConfig.OnDelete.Patch, old));
        }
        else if (!isDelete && embeddedConfig.Patch != null)
        {
            patches ??= [];
            patches.Add(new PatchEntry(embedOp.Processor.Key, embedOp.Processor.SourceColumnNames, embedOp.RawValues, embeddedConfig.Patch, old));
        }

        return updatedDoc;
    }

    /// <summary>
    /// Array operation with deferred materialization. Instead of rebuilding the array from the
    /// blittable on every op, maintains a working <see cref="DynamicJsonArray"/> per property.
    /// The first op for a property reads the existing array into the working copy; subsequent
    /// ops operate on the same working copy. This avoids O(n) array copies per op.
    ///
    /// $old is captured by cloning the matched item before modification, same as the non-deferred path.
    /// For same-PK sequential ops (e.g., update then delete), the second op sees the first op's
    /// modifications via the accumulated Modifications chain on the blittable item.
    /// </summary>
    private static BlittableJsonReaderObject ApplyArrayOperationDeferred(
        DocumentsOperationContext context,
        BlittableJsonReaderObject parentDoc, DynamicJsonValue target,
        CdcSinkEmbeddedTableConfig config, CdcSinkDocumentOp op, bool needsOld,
        ref Dictionary<string, DynamicJsonArray> arrayStates)
    {
        arrayStates ??= new Dictionary<string, DynamicJsonArray>(StringComparer.Ordinal);
        var stateKey = BuildArrayStateKey(op, config);

        if (arrayStates.TryGetValue(stateKey, out var workingArray) == false)
        {
            // First op for this array — initialize working copy from blittable
            workingArray = new DynamicJsonArray();
            if (parentDoc != null &&
                parentDoc.TryGetMember(config.PropertyName, out var existingValue) &&
                existingValue is BlittableJsonReaderArray existingArray)
            {
                foreach (var item in existingArray)
                    workingArray.Add(item);
            }
            arrayStates[stateKey] = workingArray;
            target[config.PropertyName] = workingArray;
        }

        // Scan the working array for a PK match.
        // Items may be BlittableJsonReaderObject (loaded from existing doc) or
        // DynamicJsonValue (inserted by a prior op in this same deferred batch).
        BlittableJsonReaderObject old = null;
        bool found = false;
        var items = workingArray.Items;

        for (int i = 0; i < items.Count; i++)
        {
            switch (items[i])
            {
                case BlittableJsonReaderObject item
                    when MatchesPrimaryKey(item, op.MappedData, config, op.Processor.MappedPrimaryKeyNames):
                    found = true;
                    old = needsOld ? context.ReadObject(item, "cdc-old-item") : null;

                    if (op.Operation == CdcSinkOperation.Upsert)
                    {
                        // Accumulate modifications — use ??= so multiple updates on the same item
                        // stack their changes instead of overwriting each other.
                        item.Modifications ??= new DynamicJsonValue(item);
                        foreach (var (name, value) in op.MappedData.Properties)
                            item.Modifications[name] = value;
                    }
                    else
                    {
                        items.RemoveAt(i);
                    }
                    break;

                case DynamicJsonValue djvItem
                    when MatchesPrimaryKeyDynamic(djvItem, op.MappedData, config, op.Processor.MappedPrimaryKeyNames):
                    found = true;
                    old = needsOld ? context.ReadObject(djvItem, "cdc-old-item") : null;

                    if (op.Operation == CdcSinkOperation.Upsert)
                    {
                        // Merge new values into the existing DJV in-place
                        foreach (var (newName, newValue) in op.MappedData.Properties)
                        {
                            bool replaced = false;
                            for (int j = 0; j < djvItem.Properties.Count; j++)
                            {
                                if (djvItem.Properties[j].Name == newName)
                                {
                                    djvItem.Properties[j] = (newName, newValue);
                                    replaced = true;
                                    break;
                                }
                            }
                            if (replaced == false)
                                djvItem.Properties.Add((newName, newValue));
                        }
                    }
                    else
                    {
                        items.RemoveAt(i);
                    }
                    break;

                default:
                    continue;
            }
            break;
        }

        if (found == false && op.Operation == CdcSinkOperation.Upsert)
            workingArray.Add(op.MappedData);

        return old;
    }

    /// <summary>
    /// Builds the cache key for arrayStates, including parent FK values for nested paths.
    /// For depth-1 embedded tables (e.g., Order → Lines[]), the key is just "schema.table:PropertyName".
    /// For deeper nesting (e.g., Company → Departments[] → Employees[]), the key includes
    /// the FK values that identify which parent element the array belongs to, so that
    /// employees in different departments get separate working arrays.
    /// </summary>
    private static string BuildArrayStateKey(CdcSinkDocumentOp op, CdcSinkEmbeddedTableConfig config)
    {
        var baseKey = op.Processor.Key + ":" + config.PropertyName;
        var path = op.Processor.PathFromRoot;
        if (path == null || path.Count <= 1)
            return baseKey;

        // Append FK values for each intermediate level to distinguish arrays under
        // different parent elements (e.g., Employees under Engineering vs Sales).
        var sb = new StringBuilder(baseKey);
        for (int seg = 0; seg < path.Count - 1; seg++)
        {
            var nextSeg = path[seg + 1];
            foreach (var (childFkCol, _) in nextSeg.JoinMapping)
            {
                var fkIdx = CdcSinkTableProcessor.FindColumnIndex(op.Processor.SourceColumnNames, childFkCol);
                sb.Append('/').Append(op.RawValues[fkIdx]);
            }
        }
        return sb.ToString();
    }

    /// <summary>
    /// Read-only lookup of the existing embedded item for IgnoreDeletes scenarios.
    /// Returns a cloned blittable for use as $old without modifying the parent document.
    /// </summary>
    private BlittableJsonReaderObject FindExistingEmbeddedItem(
        DocumentsOperationContext context, BlittableJsonReaderObject parentDoc, CdcSinkDocumentOp op)
    {
        var config = op.Processor.EmbeddedConfig;

        // For deeply nested embedded tables (depth >= 2), we need to navigate to the
        // correct parent level first. E.g., Company → Departments[10] → Employees[100]:
        // we must walk to the matching Department before looking up the Employee.
        var (_, navigatedParent) = NavigateToEmbeddedParent(parentDoc, null, op.Processor.PathFromRoot, op, readOnly: true);

        if (navigatedParent == null || navigatedParent.TryGetMember(config.PropertyName, out var existing) == false || existing == null)
            return null;

        switch (config.Type)
        {
            case CdcSinkRelationType.Value:
                return existing is BlittableJsonReaderObject valObj ? context.ReadObject(valObj, "cdc-old-item") : null;

            case CdcSinkRelationType.Array:
                if (existing is not BlittableJsonReaderArray arr)
                    return null;
                foreach (var item in arr)
                {
                    if (item is BlittableJsonReaderObject obj &&
                        MatchesPrimaryKey(obj, op.MappedData, config, op.Processor.MappedPrimaryKeyNames))
                        return context.ReadObject(obj, "cdc-old-item");
                }
                return null;

            case CdcSinkRelationType.Map:
                if (existing is not BlittableJsonReaderObject map)
                    return null;
                var mapKey = BuildMapKey(op.MappedData, config, op.Processor.MappedPrimaryKeyNames);
                if (map.TryGetMember(mapKey, out var entry) && entry is BlittableJsonReaderObject entryObj)
                    return context.ReadObject(entryObj, "cdc-old-item");
                return null;

            default:
                return null;
        }
    }

    /// <summary>
    /// Applies an embedded operation (upsert or delete) and returns the updated document
    /// along with the old embedded item (for updates and deletes — null for inserts).
    /// The old blittable is used to provide $old in patch scripts, enabling delta
    /// computations like: this.Total += $row.Amount - ($old?.Amount || 0)
    /// </summary>
    private (BlittableJsonReaderObject Document, BlittableJsonReaderObject Old) ApplyEmbeddedOperation(
        DocumentsOperationContext context, string documentId,
        BlittableJsonReaderObject parentDoc, CdcSinkDocumentOp op, bool needsOld = true)
    {
        parentDoc.Modifications ??= new DynamicJsonValue(parentDoc);
        var (target, targetBlittable) = NavigateToEmbeddedParent(parentDoc, parentDoc.Modifications, op.Processor.PathFromRoot, op);
        var config = op.Processor.EmbeddedConfig;

        BlittableJsonReaderObject old = null;

        switch (config.Type)
        {
            case CdcSinkRelationType.Array:
                old = ApplyArrayOperation(context, targetBlittable, target, config, op, needsOld);
                break;

            case CdcSinkRelationType.Map:
                old = ApplyMapOperation(context, targetBlittable, target, config, op, needsOld);
                break;

            case CdcSinkRelationType.Value:
                old = ApplyValueOperation(context, targetBlittable, target, config, op, needsOld);
                break;
        }

        var result = context.ReadObject(parentDoc, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        return (result, old);
    }

    /// <param name="parentDoc">
    /// The blittable at the navigated level, used to read existing embedded data.
    /// Null when the intermediate was a stub (no prior data at this level) —
    /// in that case we go straight to creating new values on the target.
    /// </param>
    private static BlittableJsonReaderObject ApplyValueOperation(
        DocumentsOperationContext context,
        BlittableJsonReaderObject parentDoc, DynamicJsonValue target,
        CdcSinkEmbeddedTableConfig config, CdcSinkDocumentOp op, bool needsOld = true)
    {
        if (op.Operation != CdcSinkOperation.Upsert)
        {
            BlittableJsonReaderObject old = null;
            if (needsOld)
            {
                // Check modifications first — a prior op in the deferred batch may have set the value
                old = target[config.PropertyName] switch
                {
                    DynamicJsonValue djv => context.ReadObject(djv, "cdc-old-item"),
                    BlittableJsonReaderObject modObj => context.ReadObject(modObj, "cdc-old-item"),
                    _ when parentDoc != null &&
                           parentDoc.TryGetMember(config.PropertyName, out var deletedValue) &&
                           deletedValue is BlittableJsonReaderObject deletedObj
                        => context.ReadObject(deletedObj, "cdc-old-item"),
                    _ => null
                };
            }
            target[config.PropertyName] = null;
            return old;
        }

        // Merge new values onto existing embedded object.
        // Check modifications first — a prior op in the deferred batch may have set a DJV here.
        var existingMod = target[config.PropertyName];
        if (existingMod is DynamicJsonValue existingDjv)
        {
            var old = needsOld ? context.ReadObject(existingDjv, "cdc-old-item") : null;
            foreach (var (newName, newValue) in op.MappedData.Properties)
            {
                bool replaced = false;
                for (int j = 0; j < existingDjv.Properties.Count; j++)
                {
                    if (existingDjv.Properties[j].Name == newName)
                    {
                        existingDjv.Properties[j] = (newName, newValue);
                        replaced = true;
                        break;
                    }
                }
                if (replaced == false)
                    existingDjv.Properties.Add((newName, newValue));
            }
            return old;
        }

        if (parentDoc != null &&
            parentDoc.TryGetMember(config.PropertyName, out var existingValue) &&
            existingValue is BlittableJsonReaderObject existingObj)
        {
            var old = needsOld ? context.ReadObject(existingObj, "cdc-old-item") : null;
            existingObj.Modifications = new DynamicJsonValue(existingObj);
            foreach (var (name, value) in op.MappedData.Properties)
                existingObj.Modifications[name] = value;
            return old;
        }

        target[config.PropertyName] = op.MappedData;
        return null; // insert, no old value
    }

    /// <returns>
    /// The matched item's blittable (before modification) when updating or deleting an existing item,
    /// or null for inserts. Used as $old in embedded patch scripts.
    /// </returns>
    private static BlittableJsonReaderObject ApplyArrayOperation(
        DocumentsOperationContext context,
        BlittableJsonReaderObject parentDoc, DynamicJsonValue target,
        CdcSinkEmbeddedTableConfig config, CdcSinkDocumentOp op, bool needsOld = true)
    {
        var newArray = new DynamicJsonArray();
        bool found = false;
        BlittableJsonReaderObject old = null;

        if (parentDoc != null &&
            parentDoc.TryGetMember(config.PropertyName, out var existingValue) &&
            existingValue is BlittableJsonReaderArray existingArray)
        {
            foreach (var arrayVal in existingArray)
            {
                if (arrayVal is BlittableJsonReaderObject item &&
                    MatchesPrimaryKey(item, op.MappedData, config, op.Processor.MappedPrimaryKeyNames))
                {
                    found = true;

                    // Clone BEFORE any modification so we have the previous state for $old.
                    old = needsOld ? context.ReadObject(item, "cdc-old-item") : null;

                    if (op.Operation == CdcSinkOperation.Upsert)
                    {
                        item.Modifications = new DynamicJsonValue(item);
                        foreach (var (name, value) in op.MappedData.Properties)
                            item.Modifications[name] = value;
                        newArray.Add(item);
                    }
                    // else Delete: skip (don't add) — old is still captured for OnDelete.Patch
                }
                else
                {
                    newArray.Add(arrayVal);
                }
            }
        }

        if (found == false && op.Operation == CdcSinkOperation.Upsert)
            newArray.Add(op.MappedData);

        target[config.PropertyName] = newArray;
        return old;
    }

    private static JsObject RowDataToJint(Jint.Engine engine, string[] names, object[] values)
    {
        var o = new JsObject(engine);
        for (int i = 0; i < names.Length && i < values.Length; i++)
        {
            o.FastSetDataProperty(names[i], values[i] switch
            {
                null or DBNull => JsValue.Null,
                byte[] bytes => Convert.ToBase64String(bytes),
                Guid guid => guid.ToString(),
                _ => JsValue.FromObject(engine, values[i])
            });
        }
        return o;
    }


    private BlittableJsonReaderObject ApplyMapOperation(
        DocumentsOperationContext context,
        BlittableJsonReaderObject parentDoc, DynamicJsonValue target,
        CdcSinkEmbeddedTableConfig config, CdcSinkDocumentOp op, bool needsOld = true)
    {
        // BuildMapKey normalizes the key (lowercased when case-insensitive),
        // so all stored map keys use the same normalization. Direct lookup works.
        var mapKey = BuildMapKey(op.MappedData, config, op.Processor.MappedPrimaryKeyNames);
        BlittableJsonReaderObject old = null;

        // Check modifications first — a prior op in the deferred batch may have set a DJV map here.
        var existingMod = target[config.PropertyName];
        if (existingMod is DynamicJsonValue djvMap)
        {
            var existingEntry = djvMap[mapKey];
            if (existingEntry != null && needsOld)
            {
                if (existingEntry is DynamicJsonValue djvEntry)
                    old = context.ReadObject(djvEntry, "cdc-old-item");
                else if (existingEntry is BlittableJsonReaderObject blitEntry)
                    old = context.ReadObject(blitEntry, "cdc-old-item");
            }

            if (op.Operation == CdcSinkOperation.Upsert)
            {
                if (existingEntry is DynamicJsonValue djvEntryToUpdate)
                {
                    foreach (var (newName, newValue) in op.MappedData.Properties)
                    {
                        bool replaced = false;
                        for (int j = 0; j < djvEntryToUpdate.Properties.Count; j++)
                        {
                            if (djvEntryToUpdate.Properties[j].Name == newName)
                            {
                                djvEntryToUpdate.Properties[j] = (newName, newValue);
                                replaced = true;
                                break;
                            }
                        }
                        if (replaced == false)
                            djvEntryToUpdate.Properties.Add((newName, newValue));
                    }
                }
                else
                {
                    djvMap[mapKey] = op.MappedData;
                }
            }
            else
            {
                // Remove the entry from Properties directly — DJV.Remove() requires a source
                // blittable, but this DJV was created by a prior op without one.
                for (int j = 0; j < djvMap.Properties.Count; j++)
                {
                    if (djvMap.Properties[j].Name == mapKey)
                    {
                        djvMap.Properties.RemoveAt(j);
                        break;
                    }
                }
            }

            return old;
        }

        if (parentDoc != null &&
            parentDoc.TryGetMember(config.PropertyName, out var existingValue) &&
            existingValue is BlittableJsonReaderObject existingMap)
        {
            existingMap.Modifications = new DynamicJsonValue(existingMap);

            BlittableJsonReaderObject existingBlitEntry = null;
            if (existingMap.TryGetMember(mapKey, out var entryValue) &&
                entryValue is BlittableJsonReaderObject entry)
            {
                existingBlitEntry = entry;
                old = needsOld ? context.ReadObject(existingBlitEntry, "cdc-old-item") : null;
            }

            if (op.Operation == CdcSinkOperation.Upsert)
            {
                if (existingBlitEntry is not null)
                {
                    existingBlitEntry.Modifications = new DynamicJsonValue(existingBlitEntry);
                    foreach (var (name, value) in op.MappedData.Properties)
                        existingBlitEntry.Modifications[name] = value;
                }
                else
                {
                    existingMap.Modifications[mapKey] = op.MappedData;
                }
            }
            else
            {
                existingMap.Modifications.Remove(mapKey);
            }

            target[config.PropertyName] = existingMap;
        }
        else
        {
            if (op.Operation == CdcSinkOperation.Upsert)
            {
                var newMap = new DynamicJsonValue();
                newMap[mapKey] = op.MappedData;
                target[config.PropertyName] = newMap;
            }
        }

        return old;
    }

    /// <summary>
    /// Compares primary key values between two DynamicJsonValue items.
    /// Used in the deferred array path where a prior op inserted a DJV that a later op targets.
    /// </summary>
    private static bool MatchesPrimaryKeyDynamic(
        DynamicJsonValue item, DynamicJsonValue candidate,
        CdcSinkEmbeddedTableConfig config, string[] mappedPrimaryKeyNames)
    {
        var stringComparison = config.CaseSensitiveKeys
            ? StringComparison.Ordinal
            : StringComparison.OrdinalIgnoreCase;

        for (int i = 0; i < mappedPrimaryKeyNames.Length; i++)
        {
            var mappedName = mappedPrimaryKeyNames[i];

            // Must check existence explicitly — DJV getter returns null both for
            // missing properties and for properties with a null value. Without this,
            // two items that both lack the PK field would incorrectly match.
            if (FindPropertyInDjv(item, mappedName, out var existingVal) == false)
                return false;

            var candidateVal = candidate[mappedName];
            if (candidateVal == null && existingVal == null)
                continue;
            if (candidateVal == null || existingVal == null)
                return false;
            if (ComparePrimaryKeyValues(existingVal, candidateVal, stringComparison) == false)
                return false;
        }
        return true;
    }

    private static bool FindPropertyInDjv(DynamicJsonValue djv, string name, out object value)
    {
        for (int i = 0; i < djv.Properties.Count; i++)
        {
            if (djv.Properties[i].Name == name)
            {
                value = djv.Properties[i].Value;
                return true;
            }
        }
        value = null;
        return false;
    }

    /// <summary>
    /// Compares primary key values between an existing blittable item and a candidate DynamicJsonValue.
    /// Respects the <see cref="CdcSinkEmbeddedTableConfig.CaseSensitiveKeys"/> setting.
    /// Uses the processor's pre-computed MappedPrimaryKeyNames to avoid per-row dictionary lookups.
    /// </summary>
    private static bool MatchesPrimaryKey(
        BlittableJsonReaderObject item, DynamicJsonValue candidate,
        CdcSinkEmbeddedTableConfig config, string[] mappedPrimaryKeyNames)
    {
        var stringComparison = config.CaseSensitiveKeys
            ? StringComparison.Ordinal
            : StringComparison.OrdinalIgnoreCase;

        for (int i = 0; i < mappedPrimaryKeyNames.Length; i++)
        {
            var mappedName = mappedPrimaryKeyNames[i];

            if (item.TryGetMember(mappedName, out var existingVal) == false)
                return false;

            var candidateVal = candidate[mappedName];
            if (candidateVal == null && existingVal == null)
                continue;
            if (candidateVal == null || existingVal == null)
                return false;

            if (ComparePrimaryKeyValues(existingVal, candidateVal, stringComparison) == false)
                return false;
        }
        return true;
    }

    private static bool ComparePrimaryKeyValues(object existingVal, object candidateVal, StringComparison stringComparison)
    {
        // Compare numerically when both sides are numbers to avoid string allocation
        if (existingVal is long existingLong)
        {
            if (candidateVal is long candidateLong)
                return existingLong == candidateLong;
            if (candidateVal is int candidateInt)
                return existingLong == candidateInt;
            if (candidateVal is double candidateDouble)
                return existingLong == candidateDouble;
        }

        if (existingVal is double existingDouble)
        {
            if (candidateVal is double candDouble)
                return existingDouble == candDouble;
            if (candidateVal is long candLong)
                return existingDouble == candLong;
            if (candidateVal is int candInt)
                return existingDouble == candInt;
        }

        if (existingVal is bool existingBool && candidateVal is bool candidateBool)
            return existingBool == candidateBool;

        return string.Equals(existingVal.ToString(), candidateVal.ToString(), stringComparison);
    }

    /// <summary>
    /// Builds the map key from primary key column values.
    /// Respects the <see cref="CdcSinkEmbeddedTableConfig.CaseSensitiveKeys"/> setting:
    /// when case-insensitive, the key is lowercased for consistent lookup.
    /// Uses the processor's pre-computed MappedPrimaryKeyNames to avoid per-row dictionary lookups.
    /// </summary>
    private string BuildMapKey(DynamicJsonValue mappedData, CdcSinkEmbeddedTableConfig config, string[] mappedPrimaryKeyNames)
    {
        _sb ??= new StringBuilder();
        _sb.Clear();
        for (int i = 0; i < mappedPrimaryKeyNames.Length; i++)
        {
            if (i > 0)
                _sb.Append('/');

            _sb.Append(mappedData[mappedPrimaryKeyNames[i]]?.ToString() ?? "");
        }

        var key = _sb.ToString();
        return config.CaseSensitiveKeys ? key : key.ToLowerInvariant();
    }

    /// <summary>
    /// Navigate the embedded path to find the parent DynamicJsonValue at the correct nesting level.
    /// For a path of [A, B, C], navigates to A.B and returns that as the target for C's property.
    /// For a single segment path [A], returns the root modifications.
    ///
    /// When an intermediate segment is an array (e.g., Departments[] in Company → Departments[] → Employees[]),
    /// the correct array element is found by matching the next segment's join column values from the CDC row
    /// against the stored items' mapped property values. For example, when navigating to Departments[] for an
    /// employee row, we read dept_id from the row and find the department element where DeptId matches.
    /// </summary>
    /// <returns>
    /// A tuple of (target DynamicJsonValue for modifications, the BlittableJsonReaderObject at the navigated level).
    /// The blittable may be null if the intermediate didn't exist and was created as a stub.
    /// </returns>
    /// <param name="readOnly">
    /// When true, only reads existing data without creating stubs or setting Modifications.
    /// Used by FindExistingEmbeddedItem to locate the parent level for $old lookups.
    /// Returns (null, blittable) — the Target is unused in read-only mode.
    /// </param>
    private static (DynamicJsonValue Target, BlittableJsonReaderObject Blittable) NavigateToEmbeddedParent(
        BlittableJsonReaderObject rootDoc, DynamicJsonValue rootModifications,
        List<EmbeddedPathSegment> path, CdcSinkDocumentOp op, bool readOnly = false)
    {
        if (path == null || path.Count <= 1)
            return (rootModifications, rootDoc);

        var current = rootModifications;
        var currentBlittable = rootDoc;

        for (int i = 0; i < path.Count - 1; i++)
        {
            var segment = path[i];
            var propName = segment.Config.PropertyName;

            // Property doesn't exist yet — e.g., Company has no Departments[] yet,
            // so create a stub object to attach the embedded data to.
            if (currentBlittable == null || currentBlittable.TryGetMember(propName, out var nested) == false)
            {
                if (readOnly)
                    return (null, null);

                var nestedMod = new DynamicJsonValue();
                current[propName] = nestedMod;
                current = nestedMod;
                currentBlittable = null;
                continue;
            }

            if (nested is BlittableJsonReaderObject nestedObj)
            {
                if (readOnly)
                {
                    currentBlittable = nestedObj;
                }
                else
                {
                    // Value or Map type — e.g., navigating into Company.Address (a single object)
                    if (nestedObj.Modifications != null)
                    {
                        current = nestedObj.Modifications;
                    }
                    else
                    {
                        var nestedMod = new DynamicJsonValue(nestedObj);
                        nestedObj.Modifications = nestedMod;
                        current = nestedMod;
                    }
                    currentBlittable = nestedObj;
                }
            }
            else if (nested is BlittableJsonReaderArray nestedArray)
            {
                // Array type — e.g., finding the right Department in Company.Departments[]
                // by matching dept_id from the CDC row against each element's DeptId property.
                // The next segment (i+1) has JoinMapping: { childFkColumn -> parentPkColumn }.
                var nextSegment = path[i + 1];
                BlittableJsonReaderObject matchedItem = null;

                foreach (var arrayVal in nestedArray)
                {
                    if (arrayVal is not BlittableJsonReaderObject candidate)
                        continue;

                    bool matches = true;
                    int pkIdx = 0;
                    foreach (var (childFkCol, _) in nextSegment.JoinMapping)
                    {
                        // Get the FK value from the CDC row by column name lookup
                        var fkIdx = CdcSinkTableProcessor.FindColumnIndex(op.Processor.SourceColumnNames, childFkCol);
                        var fkValue = op.RawValues[fkIdx];

                        // The parent PK column is stored under its mapped property name
                        var mappedName = segment.MappedPrimaryKeyNames[pkIdx++];

                        if (candidate.TryGetMember(mappedName, out var storedValue) == false)
                        {
                            matches = false;
                            break;
                        }

                        if (ComparePrimaryKeyValues(storedValue, fkValue, StringComparison.OrdinalIgnoreCase) == false)
                        {
                            matches = false;
                            break;
                        }
                    }

                    if (matches)
                    {
                        matchedItem = candidate;
                        break;
                    }
                }

                // Matched — e.g., found the Department where DeptId == the employee's dept_id
                if (matchedItem != null)
                {
                    if (readOnly == false)
                    {
                        matchedItem.Modifications ??= new DynamicJsonValue(matchedItem);
                        current = matchedItem.Modifications;
                    }
                    currentBlittable = matchedItem;
                }
                else if (readOnly)
                {
                    return (null, null);
                }
                else
                {
                    // No matching element — e.g., employee references a dept_id that doesn't
                    // exist yet in Departments[]. Create a stub and append it to the array.
                    var stubElement = new DynamicJsonValue();
                    var updatedArray = new DynamicJsonArray();
                    foreach (var existingItem in nestedArray)
                        updatedArray.Add(existingItem);
                    updatedArray.Add(stubElement);
                    current[propName] = updatedArray;
                    current = stubElement;
                    currentBlittable = null;
                }
            }
            else if (readOnly)
            {
                return (null, null);
            }
            else
            {
                // Unexpected type (e.g., a scalar where we expected an object/array) —
                // replace with a fresh object to attach embedded data to.
                var nestedMod = new DynamicJsonValue();
                current[propName] = nestedMod;
                current = nestedMod;
                currentBlittable = null;
            }
        }

        return (current, currentBlittable);
    }

    /// <summary>
    /// Runs all surviving patches in a single script invocation. Generates a combined script with
    /// a per-table function for each unique (tableName, patchScript) pair, then a dispatch function
    /// that iterates through all rows calling the appropriate table function.
    /// The script itself is pre-built once in <see cref="CdcSinkDocumentProcessor.CombinedPatchRequest"/>.
    /// </summary>
    private BlittableJsonReaderObject RunPatches(
        DocumentsOperationContext context, string documentId,
        BlittableJsonReaderObject document,
        List<PatchEntry> patches)
    {
         using (context.DocumentDatabase.Scripts.GetScriptRunner(_patchRequest, readOnly: false, out var runner))
        {
            var documentInstance = (BlittableObjectInstance)runner.Translate(context,
                new Document { Data = document, Id = context.GetLazyString(documentId) }).AsObject();
            
            using (var result = runner.Run(context, context, "execute", documentId, [documentInstance, CreateRows()]))
            {
                // explicitly called del() / put() here - don't save the returned patched document since the script
                // may have done its own put()
                if (runner.OriginalDocumentUpdatedOrDeleted)
                    return null; 
                return result.TranslateToObject(context, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }


            JsArray CreateRows()
            {
                // Build the $rows array: [{table: "orders", row: {...}, old: {...}}, ...]
                // $old is the previous embedded item (null for inserts, deletes, and root patches).
                // Enables delta computations: this.Total += $row.Amount - ($old?.Amount || 0)
                var rowsArray = new JsArray(runner.ScriptEngine, capacity: (uint)patches.Count);
                for (int i = 0; i < patches.Count; i++)
                {
                    var patch = patches[i];
                    var row = new JsObject(runner.ScriptEngine);
                    var old = patch.Old == null ? null : new BlittableObjectInstance(runner.ScriptEngine, null, patch.Old, null, null, null);

                    row.FastSetDataProperty("table", patch.TableName);
                    row.FastSetDataProperty("row", RowDataToJint(runner.ScriptEngine, patch.ColumnNames, patch.Values));
                    row.FastSetDataProperty("old", old);
                    rowsArray.Push(row);
                }
                return rowsArray;
            }
        }
    }

    private void UpdateState(DocumentsOperationContext context)
    {
        if (_lastLsn == null && _tableLoadUpdates == null)
            return;

        var stateDocId = CdcSinkTaskState.GetDocumentId(_configurationName);
        var existingDoc = _database.DocumentsStorage.Get(context, stateDocId);

        var state = existingDoc != null
            ? JsonDeserializationServer.CdcSinkTaskState(existingDoc.Data)
            : new CdcSinkTaskState { ConfigurationName = _configurationName };

        if (_lastLsn != null)
            state.LastLsn = _lastLsn;

        if (_tableLoadUpdates != null)
        {
            foreach (var (key, value) in _tableLoadUpdates)
                state.Tables[key] = value;
        }

        var stateJson = state.ToJson();
        stateJson[Constants.Documents.Metadata.Key] = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Collection] = CdcSinkTaskState.CollectionName,
        };

        using var stateBlittable = context.ReadObject(stateJson, stateDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        _database.DocumentsStorage.Put(context, stateDocId, expectedChangeVector: null, stateBlittable);
    }

    private static List<(string DocumentId, List<CdcSinkDocumentOp> Ops)> GroupByDocumentId(List<CdcSinkDocumentOp> ops)
    {
        var groups = new List<(string, List<CdcSinkDocumentOp>)>();
        var index = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var op in ops)
        {
            if (index.TryGetValue(op.DocumentId, out var idx))
            {
                groups[idx].Item2.Add(op);
            }
            else
            {
                index[op.DocumentId] = groups.Count;
                groups.Add((op.DocumentId, [op]));
            }
        }

        return groups;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
    {
        // Materialize DynamicJsonValue → BlittableJsonReaderObject for each op so the Dto is serializable.
        var serializedOps = new List<SerializedCdcSinkOp>(_ops.Count);
        for (int i = 0; i < _ops.Count; i++)
        {
            var op = _ops[i];
            BlittableJsonReaderObject mappedBlittable = null;
            if (op.MappedData != null)
                mappedBlittable = context.ReadObject(op.MappedData, op.DocumentId);

            var rawDjv = new DynamicJsonValue();
            DynamicJsonValue binaryDjv = null;
            if (op.RawValues != null && op.Processor?.SourceColumnNames != null)
            {
                var names = op.Processor.SourceColumnNames;
                var values = op.RawValues;
                for (int j = 0; j < names.Length && j < values.Length; j++)
                {
                    switch (values[j])
                    {
                        case null or DBNull:
                            rawDjv[names[j]] = null;
                            break;
                        case byte[] bytes:
                            // Store binary columns separately so we can restore them as byte[]
                            // on deserialization without ambiguity (vs regular string values).
                            binaryDjv ??= new DynamicJsonValue();
                            binaryDjv[names[j]] = Convert.ToBase64String(bytes);
                            break;
                        case Guid guid:
                            rawDjv[names[j]] = guid.ToString();
                            break;
                        default:
                            rawDjv[names[j]] = values[j];
                            break;
                    }
                }
            }
            var rawBlittable = context.ReadObject(rawDjv, "cdc-raw-data");
            var binaryBlittable = binaryDjv != null ? context.ReadObject(binaryDjv, "cdc-binary-data") : null;

            serializedOps.Add(new SerializedCdcSinkOp
            {
                Type = op.Type,
                DocumentId = op.DocumentId,
                Operation = op.Operation,
                ProcessorSchema = op.Processor?.Schema,
                ProcessorTable = op.Processor?.Table,
                MappedData = mappedBlittable,
                RawData = rawBlittable,
                BinaryData = binaryBlittable,
            });
        }

        return new Dto
        {
            Database = _database,
            Ops = serializedOps,
            ConfigurationName = _configurationName,
            LastLsn = _lastLsn,
            TableLoadUpdates = _tableLoadUpdates,
        };
    }

    public class SerializedCdcSinkOp
    {
        public CdcSinkDocumentOpType Type { get; set; }
        public string DocumentId { get; set; }
        public CdcSinkOperation Operation { get; set; }
        public string ProcessorSchema { get; set; }
        public string ProcessorTable { get; set; }
        public BlittableJsonReaderObject MappedData { get; set; }
        public BlittableJsonReaderObject RawData { get; set; }
        /// <summary>
        /// Binary columns (byte[]) serialized as Base64 strings, stored separately
        /// from RawData so they can be restored as byte[] on deserialization.
        /// </summary>
        public BlittableJsonReaderObject BinaryData { get; set; }
    }

    public class Dto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand>
    {
        public DocumentDatabase Database { get; set; }
        public List<SerializedCdcSinkOp> Ops { get; set; }
        public string ConfigurationName { get; set; }
        public string LastLsn { get; set; }
        public Dictionary<string, CdcSinkTableLoadState> TableLoadUpdates { get; set; }

        public DocumentMergedTransactionCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            database ??= Database;

            // Rebuild the document processor from the configuration so we can restore
            // the Processor reference on each op (needed for patches, embedded navigation, etc.)
            var config = database.CdcSinkLoader.Sinks?.Find(c => c.Name == ConfigurationName)
                ?? throw new InvalidOperationException(
                    $"Cannot replay CDC Sink batch: configuration '{ConfigurationName}' was not found. " +
                    "It may have been deleted since the batch was originally executed.");
            var docProcessor = new CdcSinkDocumentProcessor(config);

            var ops = new List<CdcSinkDocumentOp>(Ops.Count);
            for (int i = 0; i < Ops.Count; i++)
            {
                var serialized = Ops[i];
                var mappedDjv = serialized.MappedData != null ? new DynamicJsonValue(serialized.MappedData) : null;
                var processor = docProcessor.GetProcessor(serialized.ProcessorSchema, serialized.ProcessorTable);

                // Rebuild object[] from the serialized blittable using processor's column name order
                object[] rawValues = null;
                if (serialized.RawData != null && processor.SourceColumnNames != null)
                {
                    rawValues = new object[processor.SourceColumnNames.Length];
                    var prop = new BlittableJsonReaderObject.PropertyDetails();
                    for (int j = 0; j < serialized.RawData.Count; j++)
                    {
                        serialized.RawData.GetPropertyByIndex(j, ref prop);
                        var idx = CdcSinkTableProcessor.FindColumnIndex(processor.SourceColumnNames, prop.Name.ToString());
                        rawValues[idx] = prop.Value;
                    }

                    // Restore binary columns from the separate BinaryData blittable
                    if (serialized.BinaryData != null)
                    {
                        for (int j = 0; j < serialized.BinaryData.Count; j++)
                        {
                            serialized.BinaryData.GetPropertyByIndex(j, ref prop);
                            var idx = CdcSinkTableProcessor.FindColumnIndex(processor.SourceColumnNames, prop.Name.ToString());
                            rawValues[idx] = Convert.FromBase64String(prop.Value.ToString());
                        }
                    }
                }

                ops.Add(new CdcSinkDocumentOp
                {
                    Type = serialized.Type,
                    DocumentId = serialized.DocumentId,
                    Operation = serialized.Operation,
                    Processor = processor,
                    MappedData = mappedDjv,
                    RawValues = rawValues,
                });
            }

            return new CdcSinkBatchCommand(database, ops, ConfigurationName, LastLsn,
                tableLoadUpdates: TableLoadUpdates, patchRequest: docProcessor.CombinedPatchRequest,
                statsScope: null, statistics: null, logger: null);
        }
    }
}
