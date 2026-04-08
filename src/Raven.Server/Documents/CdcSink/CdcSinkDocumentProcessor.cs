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
    private readonly CdcSinkConfiguration _config;
    private readonly string _defaultSchema;
    private readonly Dictionary<(string Schema, string Table), CdcSinkTableProcessor> _tableIndex;

    internal RavenLogger Logger { get; set; }

    /// <summary>
    /// Pre-built patch request for all tables that have user scripts. Null if no tables have patches.
    /// </summary>
    public PatchRequest CombinedPatchRequest { get; }

    public CdcSinkDocumentProcessor(CdcSinkConfiguration config, string defaultSchema = "")
    {
        _config = config;
        _defaultSchema = defaultSchema;
        _tableIndex = new Dictionary<(string, string), CdcSinkTableProcessor>(TableKeyComparer.Instance);

        foreach (var table in config.Tables)
        {
            var schema = table.SourceTableSchema ?? defaultSchema;

            // Register the root table
            var rootKey = MakeKey(schema, table.SourceTableName);
            var rootPropertyLookup = BuildPropertyLookup(table.Columns);
            var rootProcessor = new CdcSinkTableProcessor
            {
                Key = rootKey,
                KeyOnDelete = rootKey + "__on_delete",
                Schema = schema,
                Table = table.SourceTableName,
                RootConfig = table,
                CollectionName = table.CollectionName,
                IsRoot = true,
                Columns = table.Columns,
                AttachmentColumns = FilterAttachmentColumns(table.Columns),
                PropertyLookup = rootPropertyLookup,
                MappedPrimaryKeyNames = BuildMappedPrimaryKeyNames(table.PrimaryKeyColumns, rootPropertyLookup),
                LinkedTables = table.LinkedTables,
            };

            _tableIndex[(schema, table.SourceTableName)] = rootProcessor;

            // Register all embedded tables recursively
            if (table.EmbeddedTables != null)
            {
                RegisterEmbeddedTables(table, table.EmbeddedTables, table.PrimaryKeyColumns, rootPropertyLookup, new List<EmbeddedPathSegment>(), defaultSchema);
            }
        }

        CombinedPatchRequest = BuildCombinedPatchRequest();
    }

    /// <summary>
    /// Builds a single combined script that dispatches per-table patches by table name.
     /// Each per-table function receives $row as a parameter — so user scripts
    /// can reference $row.column_name naturally, with `this` bound to the document.
    /// </summary>
    private PatchRequest BuildCombinedPatchRequest()
    {
        var tableScripts = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var table in _config.Tables)
        {
            var schema = table.SourceTableSchema ?? _defaultSchema;

            if (table.Patch != null)
                tableScripts.TryAdd(MakeKey(schema, table.SourceTableName), table.Patch);

            if (table.OnDelete?.Patch != null)
                tableScripts.TryAdd(OnDeleteKey(schema, table.SourceTableName), table.OnDelete.Patch);

            CdcSinkConfiguration.ForEachEmbeddedTable(table.EmbeddedTables, e =>
            {
                var embeddedSchema = e.SourceTableSchema ?? _defaultSchema;
                if (e.Patch != null)
                    tableScripts.TryAdd(MakeKey(embeddedSchema, e.SourceTableName), e.Patch);
                if (e.OnDelete?.Patch != null)
                    tableScripts.TryAdd(OnDeleteKey(embeddedSchema, e.SourceTableName), e.OnDelete.Patch);
            });
        }

        if (tableScripts.Count == 0)
            return null;

        var functions = new Dictionary<string, DeclaredFunction>(StringComparer.OrdinalIgnoreCase);
        var switchCases = new StringBuilder();

        foreach (var (tableName, script) in tableScripts)
        {
            var funcName = $"__cdc_{SanitizeForJs(tableName)}";

            functions[funcName] = new DeclaredFunction
            {
                Name = funcName,
                FunctionText = $"function {funcName}($row, $old) {{\n{script}\n}}",
                Type = DeclaredFunction.FunctionType.JavaScript,
            };

            switchCases.Append("    case \"").Append(EscapeJsString(tableName))
                .Append("\": ").Append(funcName).Append(".call(this, $row, $old); break;\n");
        }

        // $old is the previous embedded item data — null for inserts and root patches,
        // populated for embedded updates. Enables delta computations in scripts:
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
        Dictionary<string, string> parentPropertyLookup,
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

            var embeddedSchema = embedded.SourceTableSchema ?? defaultSchema;
            var key = MakeKey(embeddedSchema, embedded.SourceTableName);
            var embeddedPropertyLookup = BuildPropertyLookup(embedded.Columns);
            var processor = new CdcSinkTableProcessor
            {
                Key = key,
                KeyOnDelete = key + "__on_delete",
                Schema = embeddedSchema,
                Table = embedded.SourceTableName,
                RootConfig = rootConfig,
                CollectionName = rootConfig.CollectionName,
                IsRoot = false,
                EmbeddedConfig = embedded,
                PathFromRoot = path,
                RootJoinColumns = rootJoinColumns,
                Columns = embedded.Columns,
                AttachmentColumns = FilterAttachmentColumns(embedded.Columns),
                PropertyLookup = embeddedPropertyLookup,
                MappedPrimaryKeyNames = BuildMappedPrimaryKeyNames(embedded.PrimaryKeyColumns, embeddedPropertyLookup),
                LinkedTables = embedded.LinkedTables,
            };

            _tableIndex[(embeddedSchema, embedded.SourceTableName)] = processor;

            // Recurse for deep nesting
            if (embedded.EmbeddedTables != null && embedded.EmbeddedTables.Count > 0)
            {
                RegisterEmbeddedTables(rootConfig, embedded.EmbeddedTables, embedded.PrimaryKeyColumns, embeddedPropertyLookup, path, defaultSchema);
            }
        }
    }

    public CdcSinkTableProcessor GetProcessor(string schema, string table)
    {
        if (_tableIndex.TryGetValue((schema ?? "", table), out var processor) == false)
            throw new InvalidOperationException($"No processor found for table '{schema}.{table}'.");
        return processor;
    }

    public void SetSourceColumnNames(string schema, string table, string[] columnNames)
    {
        if (_tableIndex.TryGetValue((schema ?? "", table), out var processor) == false)
            throw new InvalidOperationException($"Cannot set source column names for unknown table '{schema}.{table}'.");
        processor.SetSourceColumnNames(columnNames);
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
    /// Clears the contents of pooled arrays (releases references for GC) but keeps
    /// the arrays in the pool for reuse. Use when idle for a short period.
    /// </summary>
    public void ClearValuePoolArrays()
    {
        foreach (var (_, processor) in _tableIndex)
            processor.ClearPoolArrays();
    }

    /// <summary>
    /// Releases all pooled arrays entirely. Use when idle for a longer period.
    /// </summary>
    public void ClearValuePools()
    {
        foreach (var (_, processor) in _tableIndex)
            processor.ClearPool();
    }

    public CdcSinkDocumentOp ProcessRow(CdcSinkRow row, JsonOperationContext context)
    {
        if (_tableIndex.TryGetValue((row.TableSchema ?? "", row.TableName), out var processor) == false)
        {
            if (Logger?.IsDebugEnabled == true)
                Logger.Debug($"Discarding CDC row for table '{row.TableSchema}.{row.TableName}' — not configured in the CDC Sink task.");
            return null;
        }

        return ProcessRow(processor, row.Operation, row.Data, context);
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
            var onDelete = config.OnDelete;
            if (onDelete?.IgnoreDeletes == true && onDelete.Patch == null)
                return null; // silently ignore — no patch, no delete

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
        var onDelete = processor.EmbeddedConfig.OnDelete;
        if (operation == CdcSinkOperation.Delete && onDelete?.IgnoreDeletes == true && onDelete.Patch == null)
            return null; // silently ignore — no patch, no delete

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

    /// <summary>
    /// Dispatch key for OnDelete.Patch scripts in the combined patch request,
    /// distinct from the regular Patch key for the same table.
    /// </summary>
    internal static string OnDeleteKey(string schema, string tableName) => MakeKey(schema, tableName) + "__on_delete";

    private static string MakeKey(string schema, string tableName)
    {
        if (string.IsNullOrEmpty(schema))
            return tableName;
        return schema + "." + tableName;
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
