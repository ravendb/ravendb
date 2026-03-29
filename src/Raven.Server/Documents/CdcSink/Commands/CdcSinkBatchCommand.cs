using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Jint;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.CdcSink.Commands;

public sealed class CdcSinkBatchCommand : DocumentMergedTransactionCommand
{
    private readonly DocumentDatabase _database;
    private readonly List<CdcSinkDocumentOp> _ops;
    private readonly string _configurationName;
    private readonly string _lastLsn;
    private readonly Dictionary<string, CdcSinkTableLoadState> _tableLoadUpdates;
    private readonly CdcSinkStatsScope _statsScope;
    private readonly CdcSinkProcessStatistics _statistics;
    private readonly RavenLogger _logger;
    private StringBuilder _sb;

    public int ProcessedSuccessfully { get; private set; }

    public CdcSinkBatchCommand(
        DocumentDatabase database,
        List<CdcSinkDocumentOp> ops,
        string configurationName,
        string lastLsn,
        Dictionary<string, CdcSinkTableLoadState> tableLoadUpdates,
        CdcSinkStatsScope statsScope,
        CdcSinkProcessStatistics statistics,
        RavenLogger logger)
    {
        _database = database;
        _ops = ops;
        _configurationName = configurationName;
        _lastLsn = lastLsn;
        _tableLoadUpdates = tableLoadUpdates;
        _statsScope = statsScope;
        _statistics = statistics;
        _logger = logger;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        var groups = GroupByDocumentId(_ops);

        foreach (var (documentId, ops) in groups)
        {
            try
            {
                ProcessDocumentGroup(context, documentId, ops);
                ProcessedSuccessfully += ops.Count;
                _statsScope?.RecordProcessedMessage();
            }
            catch (Exception e)
            {
                if (_logger?.IsErrorEnabled == true)
                    _logger.Error($"Failed to process CDC operations for document '{documentId}'.", e);

                _statsScope?.RecordScriptProcessingError();
                _statistics?.RecordScriptExecutionError(e);
            }
        }

        UpdateState(context);

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
        BlittableJsonReaderObject currentDoc = null;
        List<CdcSinkDocumentOp> pendingEmbeds = null;

        List<(string TableName, Dictionary<string, object> RawData, string PatchScript)> patches = null;
        CdcSinkDocumentOp lastPutOp = null;
        string collectionName = null;
        bool needsDelete = false;

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
                {
                    // Merge new values onto current state
                    if (currentDoc != null)
                    {
                        currentDoc.Modifications ??= new DynamicJsonValue(currentDoc);
                        foreach (var (name, value) in op.MappedData.Properties)
                            currentDoc.Modifications[name] = value;
                    }
                    else
                    {
                        currentDoc = context.ReadObject(op.MappedData, documentId, BlittableJsonDocumentBuilder.UsageMode.None);
                    }

                    lastPutOp = op;
                    needsDelete = false;

                    if (op.Processor.RootConfig.Patch != null)
                    {
                        patches ??= new List<(string, Dictionary<string, object>, string)>();
                        patches.Add((op.Processor.RootConfig.SourceTableName, op.RawData, op.Processor.RootConfig.Patch));
                    }

                    break;
                }
                case CdcSinkDocumentOpType.Delete:
                {
                    currentDoc = null;
                    pendingEmbeds?.Clear();
                    patches?.Clear();
                    lastPutOp = null;
                    needsDelete = true;
                    break;
                }
                case CdcSinkDocumentOpType.EmbeddedModify:
                {
                    needsDelete = false;
                    pendingEmbeds ??= new List<CdcSinkDocumentOp>();
                    pendingEmbeds.Add(op);
                    if (op.Processor.EmbeddedConfig.Patch != null)
                    {
                        patches ??= new List<(string, Dictionary<string, object>, string)>();
                        patches.Add((op.Processor.EmbeddedConfig.SourceTableName, op.RawData, op.Processor.EmbeddedConfig.Patch));
                    }
                    break;
                }
            }
        }

        if (needsDelete)
        {
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

        foreach(var embedOp in pendingEmbeds ?? [])
        {
            currentDoc = ApplyEmbeddedOperation(context, documentId, currentDoc, embedOp);
        }

        if (patches is { Count: > 0 })
        {
            currentDoc = RunPatches(context, documentId, currentDoc, patches);
        }

        var final = context.ReadObject(currentDoc, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

        _database.DocumentsStorage.Put(context, documentId, expectedChangeVector: null, final);

        if (lastPutOp != null) // Store attachments from root table binary columns
            StoreAttachments(context, documentId, lastPutOp.RawData, lastPutOp.Processor.RootConfig.AttachmentNameMapping, prefix: null);

        // Handle attachments from embedded table binary columns.
        // Attachment name includes the embedded path and PK values to distinguish
        // attachments from different embedded rows (e.g., "Lines/42/photo").
        foreach(var embOp in pendingEmbeds ?? [])
        {
            var embConfig = embOp.Processor.EmbeddedConfig;
            if (embConfig.AttachmentNameMapping == null || embConfig.AttachmentNameMapping.Count == 0)
                continue;

            var prefix = BuildEmbeddedAttachmentPrefix(embConfig, embOp.RawData);

            if (embOp.Operation == CdcSinkOperation.Upsert)
            {
                StoreAttachments(context, documentId, embOp.RawData, embConfig.AttachmentNameMapping, prefix);
            }
            else
            {
                DeleteAttachments(context, documentId, embConfig.AttachmentNameMapping, prefix);
            }
        }
    }

    private void StoreAttachments(
        DocumentsOperationContext context, string documentId,
        Dictionary<string, object> rawData, Dictionary<string, string> attachmentMapping, string prefix)
    {
        foreach (var (sqlColumn, attachmentName) in attachmentMapping)
        {
            if (rawData.TryGetValue(sqlColumn, out var value) == false || value is not byte[] bytes)
                continue;

            var name = prefix != null ? prefix + attachmentName : attachmentName;
            var hash = AttachmentsStorageHelper.CalculateHash(bytes);
            using var stream = new MemoryStream(bytes);

            _database.DocumentsStorage.AttachmentsStorage.PutAttachment(
                context, documentId, name, "application/octet-stream",
                hash, bytes.Length, remoteParams: null, stream: stream);
        }
    }

    private void DeleteAttachments(
        DocumentsOperationContext context, string documentId,
        Dictionary<string, string> attachmentMapping, string prefix)
    {
        foreach (var (_, attachmentName) in attachmentMapping)
        {
            var name = prefix != null ? prefix + attachmentName : attachmentName;
            _database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(
                context, documentId, name, expectedChangeVector: null, collectionName: out _, updateDocument: false);
        }
    }

    private string BuildEmbeddedAttachmentPrefix(CdcSinkEmbeddedTableConfig config, Dictionary<string, object> rawData)
    {
        _sb ??= new StringBuilder();
        _sb.Clear();
        _sb.Append(config.PropertyName).Append('/');
        for (int j = 0; j < config.PrimaryKeyColumns.Count; j++)
        {
            if (j > 0)
                _sb.Append('/');
            var pkCol = config.PrimaryKeyColumns[j];
            _sb.Append(rawData.TryGetValue(pkCol, out var v) ? v?.ToString() ?? "" : "");
        }
        _sb.Append('/');
        return _sb.ToString();
    }

    private BlittableJsonReaderObject ApplyEmbeddedOperation(
        DocumentsOperationContext context, string documentId,
        BlittableJsonReaderObject parentDoc, CdcSinkDocumentOp op)
    {
        parentDoc.Modifications = new DynamicJsonValue(parentDoc);
        var target = NavigateToEmbeddedParent(parentDoc, parentDoc.Modifications, op.Processor.PathFromRoot);
        var config = op.Processor.EmbeddedConfig;

        switch (config.Type)
        {
            case CdcSinkRelationType.Array:
                ApplyArrayOperation(parentDoc, target, config, op);
                break;

            case CdcSinkRelationType.Map:
                ApplyMapOperation(parentDoc, target, config, op);
                break;

            case CdcSinkRelationType.Value:
                ApplyValueOperation(parentDoc, target, config, op);
                break;
        }

        return context.ReadObject(parentDoc, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
    }

    private static void ApplyValueOperation(
        BlittableJsonReaderObject parentDoc, DynamicJsonValue target,
        CdcSinkEmbeddedTableConfig config, CdcSinkDocumentOp op)
    {
        if (op.Operation != CdcSinkOperation.Upsert)
        {
            target[config.PropertyName] = null;
            return;
        }

        // Merge new values onto existing embedded object 
        if (parentDoc.TryGetMember(config.PropertyName, out var existingValue) &&
            existingValue is BlittableJsonReaderObject existingObj)
        {
            existingObj.Modifications = new DynamicJsonValue(existingObj);
            foreach (var (name, value) in op.MappedData.Properties)
                existingObj.Modifications[name] = value;
        }
        else
        {
            target[config.PropertyName] = op.MappedData;
        }
    }

    private static void ApplyArrayOperation(
        BlittableJsonReaderObject parentDoc, DynamicJsonValue target,
        CdcSinkEmbeddedTableConfig config, CdcSinkDocumentOp op)
    {
        var newArray = new DynamicJsonArray();
        bool found = false;

        if (parentDoc.TryGetMember(config.PropertyName, out var existingValue) &&
            existingValue is BlittableJsonReaderArray existingArray)
        {
            foreach(var arrayVal in existingArray)
            {
                if (arrayVal is BlittableJsonReaderObject item &&
                    MatchesPrimaryKey(item, op.MappedData, config))
                {
                    found = true;
                    if (op.Operation == CdcSinkOperation.Upsert)
                    {
                        // Use Modifications pattern on the nested blittable object
                        item.Modifications = new DynamicJsonValue(item);
                        foreach (var (name, value) in op.MappedData.Properties)
                            item.Modifications[name] = value;
                        newArray.Add(item);
                    }
                    // else Delete: skip (don't add)
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
    }

    private void ApplyMapOperation(
        BlittableJsonReaderObject parentDoc, DynamicJsonValue target,
        CdcSinkEmbeddedTableConfig config, CdcSinkDocumentOp op)
    {
        // BuildMapKey normalizes the key (lowercased when case-insensitive),
        // so all stored map keys use the same normalization. Direct lookup works.
        var mapKey = BuildMapKey(op.MappedData, config);

        if (parentDoc.TryGetMember(config.PropertyName, out var existingValue) &&
            existingValue is BlittableJsonReaderObject existingMap)
        {
            existingMap.Modifications = new DynamicJsonValue(existingMap);

            if (op.Operation == CdcSinkOperation.Upsert)
            {
                if (existingMap.TryGetMember(mapKey, out var entryValue) &&
                    entryValue is BlittableJsonReaderObject existingEntry)
                {
                    existingEntry.Modifications = new DynamicJsonValue(existingEntry);
                    foreach (var (name, value) in op.MappedData.Properties)
                        existingEntry.Modifications[name] = value;
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
    }

    /// <summary>
    /// Compares primary key values between an existing blittable item and a candidate DynamicJsonValue.
    /// Respects the <see cref="CdcSinkEmbeddedTableConfig.CaseSensitiveKeys"/> setting.
    /// </summary>
    private static bool MatchesPrimaryKey(
        BlittableJsonReaderObject item, DynamicJsonValue candidate,
        CdcSinkEmbeddedTableConfig config)
    {
        var stringComparison = config.CaseSensitiveKeys
            ? StringComparison.Ordinal
            : StringComparison.OrdinalIgnoreCase;

        foreach (var pkCol in config.PrimaryKeyColumns)
        {
            if (config.ColumnsMapping.TryGetValue(pkCol, out var mappedName) == false)
                mappedName = pkCol;

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
    /// </summary>
    private string BuildMapKey(DynamicJsonValue mappedData, CdcSinkEmbeddedTableConfig config)
    {
        _sb ??= new StringBuilder();
        _sb.Clear();
        for (int i = 0; i < config.PrimaryKeyColumns.Count; i++)
        {
            if (i > 0)
                _sb.Append('/');
            var pkCol = config.PrimaryKeyColumns[i];
            if (config.ColumnsMapping.TryGetValue(pkCol, out var mappedName) == false)
                mappedName = pkCol;

            _sb.Append(mappedData[mappedName]?.ToString() ?? "");
        }

        var key = _sb.ToString();
        return config.CaseSensitiveKeys ? key : key.ToLowerInvariant();
    }

    /// <summary>
    /// Navigate the embedded path to find the parent DynamicJsonValue at the correct nesting level.
    /// For a path of [A, B, C], navigates to A.B and returns that as the target for C's property.
    /// For a single segment path [A], returns the root modifications.
    /// </summary>
    private static DynamicJsonValue NavigateToEmbeddedParent(
        BlittableJsonReaderObject rootDoc, DynamicJsonValue rootModifications,
        List<EmbeddedPathSegment> path)
    {
        if (path == null || path.Count <= 1)
            return rootModifications;

        var current = rootModifications;
        var currentBlittable = rootDoc;

        for (int i = 0; i < path.Count - 1; i++)
        {
            var segment = path[i];
            var propName = segment.Config.PropertyName;

            if (currentBlittable != null &&
                currentBlittable.TryGetMember(propName, out var nested) &&
                nested is BlittableJsonReaderObject nestedObj)
            {
                // Reuse existing Modifications if already set, otherwise create new ones
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
            else
            {
                var nestedMod = new DynamicJsonValue();
                current[propName] = nestedMod;
                current = nestedMod;
                currentBlittable = null;
            }
        }

        return current;
    }

    /// <summary>
    /// Runs all surviving patches in a single script invocation. Generates a combined script with
    /// a per-table function for each unique (tableName, patchScript) pair, then a dispatch function
    /// that iterates through all rows calling the appropriate table function.
    /// </summary>
    private BlittableJsonReaderObject RunPatches(
        DocumentsOperationContext context, string documentId,
        BlittableJsonReaderObject document,
        List<(string TableName, Dictionary<string, object> RawData, string PatchScript)> patches)
    {
        // Collect unique table→script pairs. Each table has exactly one script.
        // Use table name as the function identifier.
        var tableScripts = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < patches.Count; i++)
        {
            var (tableName, _, patchScript) = patches[i];
            // The same table always has the same script, so TryAdd is sufficient
            tableScripts.TryAdd(tableName, patchScript);
        }

        // Build the combined script:
        //   function __cdc_<tableName>($row) { <user script> }
        //   ...
        //   function execute(doc, args) {
        //     Raven_ExplodeArgs(this, args);
        //     var $rows = args.$rows;
        //     for (var i = 0; i < $rows.length; i++) {
        //       var $row = $rows[i].row;
        //       switch($rows[i].table) {
        //         case "<tableName>": __cdc_<tableName>.call(this, $row); break;
        //       }
        //     }
        //     return doc;
        //   }
        //
        // Each per-table function receives $row as a direct parameter.
        // The user's script references $row.column_name directly.
        // `this` is the document being modified.
        var sb = new StringBuilder();
        var switchCases = new StringBuilder();

        foreach (var kvp in tableScripts)
        {
            var funcName = $"__cdc_{SanitizeForJs(kvp.Key)}";
            sb.Append("function ").Append(funcName).Append("($row) {\n");
            sb.Append(kvp.Value).Append("\n}\n\n");

            switchCases.Append("      case \"").Append(EscapeJsString(kvp.Key))
                .Append("\": ").Append(funcName).Append(".call(doc, $row); break;\n");
        }

        sb.Append("function execute(doc, args) {\n");
        sb.Append("  var $rows = args.$rows;\n");
        sb.Append("  for (var i = 0; i < $rows.length; i++) {\n");
        sb.Append("    var $row = $rows[i].row;\n");
        sb.Append("    switch($rows[i].table) {\n");
        sb.Append(switchCases);
        sb.Append("    }\n");
        sb.Append("  }\n");
        sb.Append("  return doc;\n");
        sb.Append("}\n");

        var combinedScript = sb.ToString();

        // Build the $rows array: [{table: "orders", row: {...}}, {table: "order_lines", row: {...}}]
        var rowsArray = new DynamicJsonArray();
        for (int i = 0; i < patches.Count; i++)
        {
            var rowDjv = new DynamicJsonValue();
            foreach (var kvp in patches[i].RawData)
            {
                rowDjv[kvp.Key] = kvp.Value switch
                {
                    null or DBNull => null,
                    byte[] bytes => Convert.ToBase64String(bytes),
                    Guid guid => guid.ToString(),
                    _ => kvp.Value
                };
            }

            rowsArray.Add(new DynamicJsonValue
            {
                ["table"] = patches[i].TableName,
                ["row"] = rowDjv
            });
        }

        var argsDjv = new DynamicJsonValue { ["$rows"] = rowsArray };
        using var argsBlittable = context.ReadObject(argsDjv, "cdc-patch-args");

        // EtlBehaviorFunctions passes the script as-is (no wrapper generation)
        var patchRequest = new PatchRequest(combinedScript, PatchRequestType.EtlBehaviorFunctions);
        using (context.DocumentDatabase.Scripts.GetScriptRunner(patchRequest, readOnly: false, out var runner))
        {
            var documentInstance = (Patch.BlittableObjectInstance)runner.Translate(context,
                new Document { Data = document, Id = context.GetLazyString(documentId) }).AsObject();

            using (var result = runner.Run(context, context, "execute", documentId, new object[] { documentInstance, argsBlittable }))
            {
                return result.TranslateToObject(context, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
        }
    }

    /// <summary>
    /// Sanitize a SQL table name to be a valid JavaScript identifier.
    /// Replaces non-alphanumeric/underscore characters with underscores.
    /// </summary>
    private static string SanitizeForJs(string tableName)
    {
        var sb = new StringBuilder(tableName.Length);
        for (int i = 0; i < tableName.Length; i++)
        {
            var c = tableName[i];
            sb.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        }
        return sb.ToString();
    }

    /// <summary>
    /// Escape a string for use inside a JavaScript string literal (double-quoted).
    /// </summary>
    private static string EscapeJsString(string value)
    {
        return value.Replace("\\", "\\\\").Replace("\"", "\\\"");
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
            if (op.RawData != null)
            {
                foreach (var kvp in op.RawData)
                {
                    rawDjv[kvp.Key] = kvp.Value switch
                    {
                        null or DBNull => null,
                        byte[] bytes => Convert.ToBase64String(bytes),
                        Guid guid => guid.ToString(),
                        _ => kvp.Value
                    };
                }
            }
            var rawBlittable = context.ReadObject(rawDjv, "cdc-raw-data");

            serializedOps.Add(new SerializedCdcSinkOp
            {
                Type = op.Type,
                DocumentId = op.DocumentId,
                Operation = op.Operation,
                ProcessorKey = op.Processor?.Key,
                MappedData = mappedBlittable,
                RawData = rawBlittable,
            });
        }

        return new Dto
        {
            Database = _database,
            Ops = serializedOps,
            ConfigurationName = _configurationName,
            LastLsn = _lastLsn,
        };
    }

    public class SerializedCdcSinkOp
    {
        public CdcSinkDocumentOpType Type { get; set; }
        public string DocumentId { get; set; }
        public CdcSinkOperation Operation { get; set; }
        public string ProcessorKey { get; set; }
        public BlittableJsonReaderObject MappedData { get; set; }
        public BlittableJsonReaderObject RawData { get; set; }
    }

    public class Dto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand>
    {
        public DocumentDatabase Database { get; set; }
        public List<SerializedCdcSinkOp> Ops { get; set; }
        public string ConfigurationName { get; set; }
        public string LastLsn { get; set; }

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
                var rawData = new Dictionary<string, object>();

                if (serialized.RawData != null)
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();
                    for (int j = 0; j < serialized.RawData.Count; j++)
                    {
                        serialized.RawData.GetPropertyByIndex(j, ref prop);
                        rawData[prop.Name] = prop.Value;
                    }
                }

                ops.Add(new CdcSinkDocumentOp
                {
                    Type = serialized.Type,
                    DocumentId = serialized.DocumentId,
                    Operation = serialized.Operation,
                    Processor = docProcessor.GetProcessor(serialized.ProcessorKey),
                    MappedData = mappedDjv,
                    RawData = rawData,
                });
            }

            return new CdcSinkBatchCommand(database, ops, ConfigurationName, LastLsn,
                tableLoadUpdates: null, statsScope: null, statistics: null, logger: null);
        }
    }
}
