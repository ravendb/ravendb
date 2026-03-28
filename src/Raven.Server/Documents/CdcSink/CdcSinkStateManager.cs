using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Manages reading and writing CDC Sink task state documents in the @cdc-states collection.
/// State documents track the last processed LSN and per-table initial load progress.
/// </summary>
public class CdcSinkStateManager
{
    private readonly DocumentDatabase _database;

    public CdcSinkStateManager(DocumentDatabase database)
    {
        _database = database;
    }

    /// <summary>
    /// Loads the state document for the given configuration, or returns an empty state if none exists.
    /// </summary>
    public CdcSinkTaskState LoadState(DocumentsOperationContext context, string configurationName)
    {
        var docId = CdcSinkTaskState.GetDocumentId(configurationName);
        var doc = _database.DocumentsStorage.Get(context, docId);

        if (doc == null)
        {
            return new CdcSinkTaskState
            {
                ConfigurationName = configurationName,
            };
        }

        return DeserializeState(doc.Data);
    }

    /// <summary>
    /// Saves the state document within the provided write transaction context.
    /// </summary>
    public void SaveState(DocumentsOperationContext context, CdcSinkTaskState state)
    {
        var docId = CdcSinkTaskState.GetDocumentId(state.ConfigurationName);

        using var stateJson = context.ReadObject(state.ToJson(), docId);

        var metadata = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Collection] = CdcSinkTaskState.CollectionName,
        };

        using var metaJson = context.ReadObject(metadata, "cdc-state-meta");

        _database.DocumentsStorage.Put(context, docId, null, stateJson);
    }

    /// <summary>
    /// Marks a table's initial load as complete and clears the resume key.
    /// </summary>
    public void MarkTableLoadComplete(DocumentsOperationContext context, string configurationName, string tableKey)
    {
        var state = LoadState(context, configurationName);

        if (state.Tables.TryGetValue(tableKey, out var tableState) == false)
        {
            tableState = new CdcSinkTableLoadState();
            state.Tables[tableKey] = tableState;
        }

        tableState.InitialLoadCompleted = true;
        tableState.LastKeyValues = null;

        SaveState(context, state);
    }

    /// <summary>
    /// Updates the resume key for a table that is in progress of initial load.
    /// </summary>
    public void UpdateTableLoadProgress(DocumentsOperationContext context, string configurationName, string tableKey, List<string> lastKeyValues)
    {
        var state = LoadState(context, configurationName);

        if (state.Tables.TryGetValue(tableKey, out var tableState) == false)
        {
            tableState = new CdcSinkTableLoadState();
            state.Tables[tableKey] = tableState;
        }

        tableState.LastKeyValues = lastKeyValues;

        SaveState(context, state);
    }

    /// <summary>
    /// Updates the last successfully processed LSN.
    /// </summary>
    public void UpdateLastLsn(DocumentsOperationContext context, string configurationName, string lastLsn)
    {
        var state = LoadState(context, configurationName);
        state.LastLsn = lastLsn;
        SaveState(context, state);
    }

    /// <summary>
    /// Returns a table key in the format "schema.tableName" (lowercase).
    /// </summary>
    public static string MakeTableKey(string schema, string tableName)
    {
        if (string.IsNullOrEmpty(schema))
            return tableName.ToLowerInvariant();

        return (schema + "." + tableName).ToLowerInvariant();
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
            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < tablesJson.Count; i++)
            {
                tablesJson.GetPropertyByIndex(i, ref propertyDetails);
                var tableKey = propertyDetails.Name.ToString();

                if (propertyDetails.Value is BlittableJsonReaderObject tableJson)
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
}
