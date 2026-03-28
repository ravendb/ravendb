using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink.Stats;
using Raven.Server.Documents.Patch;
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

    private void ProcessDocumentGroup(DocumentsOperationContext context, string documentId, List<CdcSinkDocumentOp> ops)
    {
        var rootPut = ops.FirstOrDefault(o => o.Type == CdcSinkDocumentOpType.Put);
        var rootDelete = ops.FirstOrDefault(o => o.Type == CdcSinkDocumentOpType.Delete);
        var embeddedOps = ops.Where(o => o.Type == CdcSinkDocumentOpType.EmbeddedModify).ToList();

        if (rootDelete != null && rootPut == null && embeddedOps.Count == 0)
        {
            _database.DocumentsStorage.Delete(context, documentId, null);
            return;
        }

        BlittableJsonReaderObject document;

        if (rootPut != null)
        {
            document = context.ReadObject(rootPut.MappedData, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            if (rootPut.Processor.RootConfig.Patch != null)
                document = RunPatch(context, documentId, document, rootPut.RawData, rootPut.Processor.RootConfig.Patch);
        }
        else
        {
            var existing = _database.DocumentsStorage.Get(context, documentId);
            if (existing != null)
            {
                document = existing.Data;
            }
            else if (embeddedOps.Count > 0)
            {
                var collectionName = embeddedOps[0].Processor.CollectionName;
                var stub = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = collectionName,
                    }
                };
                document = context.ReadObject(stub, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
            else
            {
                return;
            }
        }

        foreach (var embOp in embeddedOps)
        {
            document = ApplyEmbeddedOperation(context, documentId, document, embOp);

            if (embOp.Processor.EmbeddedConfig.Patch != null)
                document = RunPatch(context, documentId, document, embOp.RawData, embOp.Processor.EmbeddedConfig.Patch);
        }

        _database.DocumentsStorage.Put(context, documentId, expectedChangeVector: null, document);

        if (rootPut != null)
            StoreAttachments(context, documentId, rootPut);
    }

    private void StoreAttachments(DocumentsOperationContext context, string documentId, CdcSinkDocumentOp op)
    {
        var attachmentMapping = op.Processor.RootConfig.AttachmentNameMapping;
        if (attachmentMapping == null || attachmentMapping.Count == 0)
            return;

        foreach (var (sqlColumn, attachmentName) in attachmentMapping)
        {
            if (op.RawData.TryGetValue(sqlColumn, out var value) == false || value is not byte[] bytes)
                continue;

            var hash = AttachmentsStorageHelper.CalculateHash(bytes);
            using var stream = new MemoryStream(bytes);

            _database.DocumentsStorage.AttachmentsStorage.PutAttachment(
                context, documentId, attachmentName, "application/octet-stream",
                hash, bytes.Length, remoteParams: null, stream: stream);
        }
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
                target[config.PropertyName] = op.Operation == CdcSinkOperation.Upsert
                    ? op.MappedData
                    : null;
                break;
        }

        return context.ReadObject(parentDoc, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
    }

    private void ApplyArrayOperation(
        BlittableJsonReaderObject parentDoc, DynamicJsonValue target,
        CdcSinkEmbeddedTableConfig config, CdcSinkDocumentOp op)
    {
        var newArray = new DynamicJsonArray();
        bool found = false;

        if (parentDoc.TryGetMember(config.PropertyName, out var existingValue) &&
            existingValue is BlittableJsonReaderArray existingArray)
        {
            for (int i = 0; i < existingArray.Length; i++)
            {
                if (existingArray[i] is BlittableJsonReaderObject item &&
                    MatchesPrimaryKey(item, op.MappedData, config))
                {
                    found = true;
                    if (op.Operation == CdcSinkOperation.Upsert)
                        newArray.Add(op.MappedData);
                    // else Delete: skip (don't add)
                }
                else
                {
                    newArray.Add(existingArray[i]);
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
        var mapKey = BuildMapKey(op.MappedData, config);

        var newMap = new DynamicJsonValue();

        if (parentDoc.TryGetMember(config.PropertyName, out var existingValue) &&
            existingValue is BlittableJsonReaderObject existingMap)
        {
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < existingMap.Count; i++)
            {
                existingMap.GetPropertyByIndex(i, ref prop);
                var key = prop.Name.ToString();
                if (key != mapKey)
                    newMap[key] = prop.Value;
            }
        }

        if (op.Operation == CdcSinkOperation.Upsert)
            newMap[mapKey] = op.MappedData;

        target[config.PropertyName] = newMap;
    }

    private static bool MatchesPrimaryKey(
        BlittableJsonReaderObject item, DynamicJsonValue candidate,
        CdcSinkEmbeddedTableConfig config)
    {
        foreach (var pkCol in config.PrimaryKeyColumns)
        {
            if (config.ColumnsMapping.TryGetValue(pkCol, out var mappedName) == false)
                mappedName = pkCol;

            if (item.TryGetMember(mappedName, out var existingVal) == false)
                return false;

            var candidateVal = candidate.Properties.FirstOrDefault(p => p.Name == mappedName);
            if (candidateVal == default)
                return false;

            if (existingVal?.ToString() != candidateVal.Value?.ToString())
                return false;
        }
        return true;
    }

    private static string BuildMapKey(DynamicJsonValue mappedData, CdcSinkEmbeddedTableConfig config)
    {
        var parts = new string[config.PrimaryKeyColumns.Count];
        for (int i = 0; i < config.PrimaryKeyColumns.Count; i++)
        {
            var pkCol = config.PrimaryKeyColumns[i];
            if (config.ColumnsMapping.TryGetValue(pkCol, out var mappedName) == false)
                mappedName = pkCol;

            var prop = mappedData.Properties.FirstOrDefault(p => p.Name == mappedName);
            parts[i] = prop.Value?.ToString() ?? "";
        }
        return string.Join(",", parts);
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

            if (currentBlittable.TryGetMember(propName, out var nested) && nested is BlittableJsonReaderObject nestedObj)
            {
                var nestedMod = new DynamicJsonValue(nestedObj);
                current[propName] = nestedMod;
                current = nestedMod;
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

    private BlittableJsonReaderObject RunPatch(
        DocumentsOperationContext context, string documentId,
        BlittableJsonReaderObject document, Dictionary<string, object> rawRow, string patchScript)
    {
        var rowDjv = new DynamicJsonValue();
        foreach (var kvp in rawRow)
        {
            rowDjv[kvp.Key] = kvp.Value switch
            {
                null or DBNull => null,
                byte[] bytes => Convert.ToBase64String(bytes),
                Guid guid => guid.ToString(),
                _ => kvp.Value
            };
        }

        var argsDjv = new DynamicJsonValue { ["$row"] = rowDjv };
        using var argsBlittable = context.ReadObject(argsDjv, "cdc-patch-args");

        var patchRequest = new PatchRequest(patchScript, PatchRequestType.CdcSink);
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

    private void UpdateState(DocumentsOperationContext context)
    {
        if (_lastLsn == null && _tableLoadUpdates == null)
            return;

        var stateDocId = CdcSinkTaskState.GetDocumentId(_configurationName);
        var existingDoc = _database.DocumentsStorage.Get(context, stateDocId);

        var state = existingDoc != null
            ? DeserializeState(existingDoc.Data)
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

    private static CdcSinkTaskState DeserializeState(BlittableJsonReaderObject data)
    {
        var state = new CdcSinkTaskState();

        if (data.TryGet(nameof(CdcSinkTaskState.ConfigurationName), out string configName))
            state.ConfigurationName = configName;

        if (data.TryGet(nameof(CdcSinkTaskState.LastLsn), out string lastLsn))
            state.LastLsn = lastLsn;

        if (data.TryGet(nameof(CdcSinkTaskState.Tables), out BlittableJsonReaderObject tablesJson) && tablesJson != null)
        {
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < tablesJson.Count; i++)
            {
                tablesJson.GetPropertyByIndex(i, ref prop);
                var tableKey = prop.Name.ToString();

                if (prop.Value is BlittableJsonReaderObject tableJson)
                {
                    var tableState = new CdcSinkTableLoadState();

                    if (tableJson.TryGet(nameof(CdcSinkTableLoadState.InitialLoadCompleted), out bool completed))
                        tableState.InitialLoadCompleted = completed;

                    if (tableJson.TryGet(nameof(CdcSinkTableLoadState.LastKeyValues), out BlittableJsonReaderArray keysArray) && keysArray != null)
                    {
                        tableState.LastKeyValues = new List<string>();
                        foreach (var key in keysArray)
                            tableState.LastKeyValues.Add(key?.ToString());
                    }

                    state.Tables[tableKey] = tableState;
                }
            }
        }

        return state;
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
                groups.Add((op.DocumentId, new List<CdcSinkDocumentOp> { op }));
            }
        }

        return groups;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
    {
        return null;
    }
}
