using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Routes CDC rows to per-table processors and produces document operations.
/// </summary>
public class CdcSinkDocumentProcessor
{
    private readonly bool _includeDisabledTables;
    private readonly Dictionary<(string Schema, string Table), List<CdcSinkTableProcessor>> _tableIndex;

    internal RavenLogger Logger { get; set; }

    /// <summary>
    /// Pre-built patch request for all tables that have user scripts. Null if no tables have patches.
    /// </summary>
    public PatchRequest CombinedPatchRequest { get; }

    /// <param name="includeDisabledTables">
    /// When false (runtime default), tables marked <see cref="CdcSinkTableConfig.Disabled"/> are not registered,
    /// so their streamed rows resolve to no processor and are discarded. Replay (<see cref="Commands.CdcSinkBatchCommand"/>)
    /// and the test/preview endpoint pass true because they must resolve every configured table regardless of state.
    /// </param>
    public CdcSinkDocumentProcessor(CdcSinkConfiguration config, string defaultSchema = "", bool includeDisabledTables = false)
    {
        _includeDisabledTables = includeDisabledTables;
        _tableIndex = new Dictionary<(string, string), List<CdcSinkTableProcessor>>(TableKeyComparer.Instance);

        // Two passes: all root mappings register before any embedded one, so processors[0] of every
        // table's list is the root whenever the table has one. Index 0 is the table's PRIMARY
        // processor - it owns the decode-buffer pool and represents the table in the
        // single-representative accessors.
        foreach (var table in config.Tables)
        {
            // A disabled table is excluded from the runtime mapping: no processor is registered, so its rows
            // are discarded during streaming and it is never initial-loaded (see CollectAllTablesFlat).
            if (table.Disabled && _includeDisabledTables == false)
                continue;

            // IsNullOrEmpty rather than `??` so callers that supply "" (empty) get the same
            // default-schema substitution as null. The test endpoint resolves SourceTableSchema
            // via string.IsNullOrEmpty before looking up tables in _tableIndex; this keeps the
            // index keys in sync. Production CDC benefits too — a saved task with empty schema
            // now indexes consistently.
            var schema = string.IsNullOrEmpty(table.SourceTableSchema) ? defaultSchema : table.SourceTableSchema;

            var discriminator = BuildDiscriminator(table.CollectionName, path: null);
            var dispatchKey = MakeKey(schema, table.SourceTableName) + "|" + discriminator;
            var rootPropertyLookup = BuildPropertyLookup(table.Columns);
            var rootProcessor = new CdcSinkTableProcessor
            {
                Key = dispatchKey,
                KeyOnDelete = dispatchKey + "__on_delete",
                Schema = schema,
                Table = table.SourceTableName,
                Discriminator = discriminator,
                RootConfig = table,
                CollectionName = table.CollectionName,
                IsRoot = true,
                IgnoresDeletes = table.OnDelete?.IgnoreDeletes == true && table.OnDelete.Patch == null,
                Columns = table.Columns,
                AttachmentColumns = FilterAttachmentColumns(table.Columns),
                PropertyLookup = rootPropertyLookup,
                MappedPrimaryKeyNames = BuildMappedPrimaryKeyNames(table.PrimaryKeyColumns, rootPropertyLookup),
                LinkedTables = table.LinkedTables,
            };

            AddProcessor(schema, table.SourceTableName, rootProcessor);
        }

        foreach (var table in config.Tables)
        {
            if (table.Disabled && _includeDisabledTables == false)
                continue;

            if (table.EmbeddedTables != null)
                RegisterEmbeddedTables(table, table.EmbeddedTables, table.PrimaryKeyColumns, new List<EmbeddedPathSegment>(), defaultSchema);
        }

        CombinedPatchRequest = BuildCombinedPatchRequest();
    }

    /// <summary>
    /// Builds a single combined script that dispatches per-mapping patches by processor Key.
    /// Each per-mapping function receives $row as a parameter — so user scripts
    /// can reference $row.column_name naturally, with `this` bound to the document.
    /// Built from the registered processors so registration uses the same per-mapping keys the
    /// dispatch path emits — a source table mapped several ways runs each mapping's own script.
    /// </summary>
    private PatchRequest BuildCombinedPatchRequest()
    {
        var tableScripts = new List<(string Key, string Script)>();

        foreach (var (_, processors) in _tableIndex)
        {
            foreach (var processor in processors)
            {
                var patch = processor.IsRoot ? processor.RootConfig.Patch : processor.EmbeddedConfig.Patch;
                if (patch != null)
                    tableScripts.Add((processor.Key, patch));

                var onDelete = processor.IsRoot ? processor.RootConfig.OnDelete : processor.EmbeddedConfig.OnDelete;
                if (onDelete?.Patch != null)
                    tableScripts.Add((processor.KeyOnDelete, onDelete.Patch));
            }
        }

        if (tableScripts.Count == 0)
            return null;

        var functions = new Dictionary<string, DeclaredFunction>(StringComparer.OrdinalIgnoreCase);
        var switchCases = new StringBuilder();

        for (int i = 0; i < tableScripts.Count; i++)
        {
            var (key, script) = tableScripts[i];
            // The counter keeps function names unique when distinct keys sanitize to the same string.
            var funcName = $"__cdc_{i}_{SanitizeForJs(key)}";

            functions[funcName] = new DeclaredFunction
            {
                Name = funcName,
                FunctionText = $"function {funcName}($row, $old) {{\n{script}\n}}",
                Type = DeclaredFunction.FunctionType.JavaScript,
            };

            switchCases.Append("    case \"").Append(EscapeJsString(key))
                .Append("\": ").Append(funcName).Append(".call(this, $row, $old); break;\n");
        }

        // $old is the previous value before the change — null for inserts of new
        // documents/items, populated for updates and deletes (both root and embedded).
        // Enables delta computations in scripts:
        //   this.Total += $row.Amount - ($old?.Amount || 0)
        var dispatchScript = $$"""
            for (var i = 0; i < rows.length; i++) {
              var $row = rows[i].row;
              var $old = rows[i].old || null;
              switch(rows[i].table) {
              {{switchCases}}
                  default: throw new Error('CDC Sink: no patch function for table "' + rows[i].table + '"'); break;
              }
            }
            """;

        return new PatchRequest(dispatchScript, PatchRequestType.CdcSink, functions);
    }

    private static string SanitizeForJs(string name)
    {
        var sb = new StringBuilder(name.Length);
        for (int i = 0; i < name.Length; i++)
        {
            var c = name[i];
            sb.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        }
        return sb.ToString();
    }

    private static string EscapeJsString(string s) => System.Text.Encodings.Web.JavaScriptEncoder.Default.Encode(s);

    private void RegisterEmbeddedTables(
        CdcSinkTableConfig rootConfig,
        List<CdcSinkEmbeddedTableConfig> embeddedTables,
        List<string> parentPkColumns,
        List<EmbeddedPathSegment> currentPath,
        string defaultSchema)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (embeddedTables == null)
            return;

        foreach (var embedded in embeddedTables)
        {
            // Build the join mapping: FK column in child → PK column in parent
            var joinMapping = new Dictionary<string, string>();
            for (int i = 0; i < embedded.JoinColumns.Count && i < parentPkColumns.Count; i++)
            {
                joinMapping[embedded.JoinColumns[i]] = parentPkColumns[i];
            }

            var segmentLookup = BuildPropertyLookup(embedded.Columns);
            var segment = new EmbeddedPathSegment
            {
                Config = embedded,
                JoinMapping = joinMapping,
                // Map this level's PK columns through its own property lookup
                // (e.g., dept_id → DeptId for finding the right department in the array)
                MappedPrimaryKeyNames = BuildMappedPrimaryKeyNames(embedded.PrimaryKeyColumns, segmentLookup),
            };

            var path = new List<EmbeddedPathSegment>(currentPath) { segment };

            // For the root join columns, we need the FK columns that map to the ROOT table's PK.
            // For single-level embedding, these are the embedded table's JoinColumns.
            // For deep nesting, the child must have denormalized FKs to the root.
            // We use the first segment's JoinColumns as the root join columns.
            //
            // Example: 4-level nesting — Company → Department → Team → Employee
            //
            //   SQL tables:
            //     companies      (PK: company_id)
            //     departments    (PK: dept_id, FK: company_id → companies)
            //     teams          (PK: team_id, FK: dept_id → departments, FK: company_id → companies)  ← denormalized
            //     employees      (PK: emp_id,  FK: team_id → teams,       FK: company_id → companies)  ← denormalized
            //
            //   Config nesting:  companies → departments → teams → employees
            //     departments.JoinColumns = ["company_id"]   (maps to companies.PK)
            //     teams.JoinColumns       = ["dept_id"]      (maps to departments.PK)
            //     employees.JoinColumns   = ["team_id"]      (maps to teams.PK)
            //
            //   At runtime, when a CDC row arrives for 'employees':
            //     PathFromRoot = [departments-segment, teams-segment, employees-segment]
            //     RootJoinColumns = path[0].Config.JoinColumns = ["company_id"]  (departments' FK to root)
            //     → We read company_id from the employee row (denormalized FK) to find the parent doc ID
            //     → The path segments tell us where to navigate: doc.Departments[dept_id].Teams[team_id].Employees[emp_id]
            //
            //   This requires that ALL descendant tables carry the root's FK (company_id) as a denormalized column.
            var rootJoinColumns = path[0].Config.JoinColumns;

            var embeddedSchema = string.IsNullOrEmpty(embedded.SourceTableSchema) ? defaultSchema : embedded.SourceTableSchema;
            var discriminator = BuildDiscriminator(rootConfig.CollectionName, path);
            var dispatchKey = MakeKey(embeddedSchema, embedded.SourceTableName) + "|" + discriminator;
            var embeddedPropertyLookup = BuildPropertyLookup(embedded.Columns);
            var processor = new CdcSinkTableProcessor
            {
                Key = dispatchKey,
                KeyOnDelete = dispatchKey + "__on_delete",
                Schema = embeddedSchema,
                Table = embedded.SourceTableName,
                Discriminator = discriminator,
                RootConfig = rootConfig,
                CollectionName = rootConfig.CollectionName,
                IsRoot = false,
                IgnoresDeletes = embedded.OnDelete?.IgnoreDeletes == true && embedded.OnDelete.Patch == null,
                EmbeddedConfig = embedded,
                PathFromRoot = path,
                RootJoinColumns = rootJoinColumns,
                Columns = embedded.Columns,
                AttachmentColumns = FilterAttachmentColumns(embedded.Columns),
                PropertyLookup = embeddedPropertyLookup,
                MappedPrimaryKeyNames = BuildMappedPrimaryKeyNames(embedded.PrimaryKeyColumns, embeddedPropertyLookup),
                LinkedTables = embedded.LinkedTables,
            };

            AddProcessor(embeddedSchema, embedded.SourceTableName, processor);

            // Recurse for deep nesting
            if (embedded.EmbeddedTables != null && embedded.EmbeddedTables.Count > 0)
            {
                RegisterEmbeddedTables(rootConfig, embedded.EmbeddedTables, embedded.PrimaryKeyColumns, path, defaultSchema);
            }
        }
    }

    public IReadOnlyList<CdcSinkTableProcessor> GetProcessors(string schema, string table)
    {
        if (_tableIndex.TryGetValue((schema ?? string.Empty, table), out var processors) == false)
            throw new InvalidOperationException($"No processor found for table '{schema}.{table}'.");
        return processors;
    }

    /// <summary>
    /// Like <see cref="GetProcessors"/> but returns false instead of throwing when the table is not
    /// configured. Used by streaming providers (e.g. PostgreSQL) where the source may publish rows for
    /// tables that are not part of the task configuration - those rows must be skipped, not crash.
    /// </summary>
    private bool TryGetProcessors(string schema, string table, out IReadOnlyList<CdcSinkTableProcessor> processors)
    {
        if (_tableIndex.TryGetValue((schema ?? string.Empty, table), out var list))
        {
            processors = list;
            return true;
        }

        processors = null;
        return false;
    }

    /// <summary>
    /// True when the source table is configured in this task. Existence-only check for streaming
    /// providers that must skip rows of published-but-unconfigured tables.
    /// </summary>
    public bool HasProcessors(string schema, string table)
    {
        return _tableIndex.ContainsKey((schema ?? string.Empty, table));
    }

    /// <summary>
    /// The primary processor for a source table: processors[0], which registration order guarantees is
    /// the root processor when the table is mapped as a collection, otherwise the first embedded
    /// processor. NOT for row routing - anything that turns a source row into document ops must use
    /// <see cref="GetProcessors"/> and fan out, or it silently drops the table's other mappings. This
    /// single-representative accessor is only for operations about the table itself where any one
    /// processor suffices: existence checks, the dry-run preview, and tests.
    /// </summary>
    public CdcSinkTableProcessor GetPrimaryProcessor(string schema, string table)
    {
        return GetProcessors(schema, table)[0];
    }

    public bool TryGetPrimaryProcessor(string schema, string table, out CdcSinkTableProcessor processor)
    {
        if (TryGetProcessors(schema, table, out var processors))
        {
            processor = processors[0];
            return true;
        }

        processor = null;
        return false;
    }

    /// <summary>
    /// Resolves the exact processor for a source table by its <see cref="CdcSinkTableProcessor.Discriminator"/>.
    /// Used by tx-log replay to restore the specific mapping an op belonged to. An op persisted without a
    /// discriminator resolves to the primary processor; an unknown discriminator throws - the mapping
    /// changed between persist and replay, and silently picking another processor would misroute the op.
    /// </summary>
    public CdcSinkTableProcessor GetProcessor(string schema, string table, string discriminator)
    {
        var processors = GetProcessors(schema, table);
        if (string.IsNullOrEmpty(discriminator))
            return processors[0];

        for (int i = 0; i < processors.Count; i++)
        {
            if (string.Equals(processors[i].Discriminator, discriminator, StringComparison.Ordinal))
                return processors[i];
        }

        throw new InvalidOperationException(
            $"No processor with discriminator '{discriminator}' found for table '{schema}.{table}'. " +
            "The table mapping changed between when the batch was persisted and this replay.");
    }

    public void SetSourceColumnNames(string schema, string table, string[] columnNames)
    {
        if (_tableIndex.TryGetValue((schema ?? string.Empty, table), out var processors) == false)
            throw new InvalidOperationException($"Cannot set source column names for unknown table '{schema}.{table}'.");

        // Every processor for this source table shares the same source columns - set them all so each
        // one recomputes its own index arrays (a doubly-mapped table has a root and embedded processor).
        for (int i = 0; i < processors.Count; i++)
            processors[i].SetSourceColumnNames(columnNames);
    }

    /// <summary>
    /// Returns all row value arrays from the completed batch back to their per-table pools.
    /// Called after the TxMerger finishes processing a batch.
    /// </summary>
    public void ReturnBatchValues(List<CdcSinkDocumentOp> ops)
    {
        foreach (var op in ops)
        {
            if (op?.RawValues != null && op.Processor != null)
                op.Processor.ReturnValues(op.RawValues);
        }
    }

    /// <summary>
    /// Releases all pooled arrays entirely. Use when idle for a longer period.
    /// </summary>
    public void ClearValuePools()
    {
        foreach (var (_, processors) in _tableIndex)
            for (int i = 0; i < processors.Count; i++)
                processors[i].ClearPool();
    }

    /// <summary>
    /// Drops every table's learned column set so the next read re-learns it. Called on retry so a
    /// schema change seen mid-initial-load doesn't keep re-failing on stale column metadata.
    /// </summary>
    public void ResetSourceColumnNames()
    {
        foreach (var (_, processors) in _tableIndex)
            for (int i = 0; i < processors.Count; i++)
                processors[i].ResetSourceColumnNames();
    }

    /// <summary>
    /// Processes a single row against the primary processor for its source table and returns one op.
    /// Kept internal for tests that map each table once; streaming and initial load fan a row out to
    /// every processor via <see cref="GetProcessors"/> so all mappings are produced.
    /// </summary>
    internal CdcSinkDocumentOp ProcessRow(CdcSinkRow row, JsonOperationContext context)
    {
        if (_tableIndex.TryGetValue((row.TableSchema ?? string.Empty, row.TableName), out var processors) == false)
        {
            if (Logger?.IsDebugEnabled == true)
                Logger.Debug($"Discarding CDC row for table '{row.TableSchema}.{row.TableName}' — not configured in the CDC Sink task.");
            return null;
        }

        return ProcessRow(processors[0], row.Operation, row.Data, context);
    }

    public CdcSinkDocumentOp ProcessRow(CdcSinkTableProcessor processor, CdcSinkOperation operation, object[] data, JsonOperationContext context)
    {
        if (processor.IsRoot)
            return ProcessRootRow(processor, operation, data, context);

        return ProcessEmbeddedRow(processor, operation, data, context);
    }

    private CdcSinkDocumentOp ProcessRootRow(CdcSinkTableProcessor processor, CdcSinkOperation operation, object[] data, JsonOperationContext context)
    {
        var config = processor.RootConfig;
        var documentId = processor.GenerateDocumentId(data);

        if (operation == CdcSinkOperation.Delete)
        {
            if (processor.IgnoresDeletes)
                return null;

            return new CdcSinkDocumentOp
            {
                Type = CdcSinkDocumentOpType.Delete,
                DocumentId = documentId,
                Processor = processor,
                Operation = CdcSinkOperation.Delete,
                RawValues = data,
            };
        }

        var mappedData = processor.MapColumns(data, context);
        processor.ApplyLinks(mappedData, data);

        mappedData[Constants.Documents.Metadata.Key] = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Collection] = config.CollectionName,
        };

        return new CdcSinkDocumentOp
        {
            Type = CdcSinkDocumentOpType.Put,
            DocumentId = documentId,
            Processor = processor,
            MappedData = mappedData,
            RawValues = data,
            Operation = CdcSinkOperation.Upsert,
        };
    }

    private CdcSinkDocumentOp ProcessEmbeddedRow(CdcSinkTableProcessor processor, CdcSinkOperation operation, object[] data, JsonOperationContext context)
    {
        if (operation == CdcSinkOperation.Delete && processor.IgnoresDeletes)
            return null;

        var parentDocumentId = processor.GetParentDocumentId(data);
        var mappedData = processor.MapColumns(data, context);
        processor.ApplyLinks(mappedData, data);

        return new CdcSinkDocumentOp
        {
            Type = CdcSinkDocumentOpType.EmbeddedModify,
            DocumentId = parentDocumentId,
            Processor = processor,
            MappedData = mappedData,
            RawValues = data,
            Operation = operation,
        };
    }

    private static string MakeKey(string schema, string tableName)
    {
        if (string.IsNullOrEmpty(schema))
            return tableName;
        return schema + "." + tableName;
    }

    private void AddProcessor(string schema, string table, CdcSinkTableProcessor processor)
    {
        var key = (schema ?? string.Empty, table);
        if (_tableIndex.TryGetValue(key, out var list) == false)
            _tableIndex[key] = list = new List<CdcSinkTableProcessor>(1);
        list.Add(processor);
    }

    /// <summary>
    /// Builds the mapping identity described on <see cref="CdcSinkTableProcessor.Discriminator"/>:
    /// the root collection name, followed by the embedded property path when <paramref name="path"/>
    /// is non-null. Escaping keeps segment boundaries unambiguous for any collection/property name.
    /// </summary>
    private static string BuildDiscriminator(string collectionName, List<EmbeddedPathSegment> path)
    {
        var sb = new StringBuilder();
        AppendEscaped(sb, collectionName);

        if (path != null)
        {
            for (int i = 0; i < path.Count; i++)
            {
                sb.Append('/');
                AppendEscaped(sb, path[i].Config.PropertyName);
            }
        }

        return sb.ToString();

        static void AppendEscaped(StringBuilder sb, string segment)
        {
            foreach (var c in segment)
            {
                if (c is '/' or '\\')
                    sb.Append('\\');
                sb.Append(c);
            }
        }
    }

    private static List<CdcColumnMapping> FilterAttachmentColumns(List<CdcColumnMapping> columns)
    {
        List<CdcColumnMapping> result = null;
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].Type == CdcColumnType.Attachment)
            {
                result ??= new List<CdcColumnMapping>();
                result.Add(columns[i]);
            }
        }
        return result ?? new List<CdcColumnMapping>();
    }

    private static string[] BuildMappedPrimaryKeyNames(List<string> primaryKeyColumns, Dictionary<string, string> propertyLookup)
    {
        var mapped = new string[primaryKeyColumns.Count];
        for (int i = 0; i < primaryKeyColumns.Count; i++)
        {
            var pkCol = primaryKeyColumns[i];
            mapped[i] = propertyLookup.TryGetValue(pkCol, out var name) ? name : pkCol;
        }
        return mapped;
    }

    private static Dictionary<string, string> BuildPropertyLookup(List<CdcColumnMapping> columns)
    {
        var lookup = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columns.Count; i++)
        {
            var col = columns[i];
            if (col.Type != CdcColumnType.Attachment)
                lookup[col.Column] = col.Name;
        }
        return lookup;
    }
}
