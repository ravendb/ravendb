using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Routes CDC rows to per-table processors and produces document operations.
/// </summary>
public class CdcSinkDocumentProcessor
{
    private readonly CdcSinkConfiguration _config;
    private readonly Dictionary<string, CdcSinkTableProcessor> _tableIndex;

    /// <summary>
    /// Pre-built patch request for all tables that have user scripts. Null if no tables have patches.
    /// </summary>
    public PatchRequest CombinedPatchRequest { get; }

    public CdcSinkDocumentProcessor(CdcSinkConfiguration config)
    {
        _config = config;
        _tableIndex = new Dictionary<string, CdcSinkTableProcessor>(StringComparer.OrdinalIgnoreCase);

        foreach (var table in config.Tables)
        {
            // Register the root table
            var rootKey = MakeKey(table.SourceTableSchema, table.SourceTableName);
            var rootProcessor = new CdcSinkTableProcessor
            {
                Key = rootKey,
                RootConfig = table,
                CollectionName = table.Name,
                IsRoot = true,
            };

            _tableIndex[rootKey] = rootProcessor;

            // Register all embedded tables recursively
            if (table.EmbeddedTables != null)
            {
                RegisterEmbeddedTables(table, table.EmbeddedTables, table.PrimaryKeyColumns, new List<EmbeddedPathSegment>());
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
            if (table.Patch != null)
                tableScripts.TryAdd(table.SourceTableName, table.Patch);

            // OnDelete.Patch scripts run separately via RunPatchOnDelete as a
            // side-effect before the delete proceeds. They are not part of the
            // combined dispatch.

            CdcSinkConfiguration.ForEachEmbeddedTable(table.EmbeddedTables, e =>
            {
                if (e.Patch != null)
                    tableScripts.TryAdd(e.SourceTableName, e.Patch);
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
            for (var i = 0; i < $rows.length; i++) {
              var $row = $rows[i].row;
              var $old = $rows[i].old || null;
              switch($rows[i].table) {
              {{switchCases}}
                  default: throw new Error('CDC Sink: no patch function for table "' + $rows[i].table + '"'); break;
              }
            }
            """;

        return new PatchRequest(dispatchScript, PatchRequestType.Patch, functions);
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

    private static string EscapeJsString(string s) => s.Replace("\\", "\\\\").Replace("\"", "\\\"");

    private void RegisterEmbeddedTables(
        CdcSinkTableConfig rootConfig,
        List<CdcSinkEmbeddedTableConfig> embeddedTables,
        List<string> parentPkColumns,
        List<EmbeddedPathSegment> currentPath)
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

            var segment = new EmbeddedPathSegment
            {
                Config = embedded,
                JoinMapping = joinMapping,
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

            var key = MakeKey(embedded.SourceTableSchema, embedded.SourceTableName);
            var processor = new CdcSinkTableProcessor
            {
                Key = key,
                RootConfig = rootConfig,
                CollectionName = rootConfig.Name,
                IsRoot = false,
                EmbeddedConfig = embedded,
                PathFromRoot = path,
                RootJoinColumns = rootJoinColumns,
            };

            _tableIndex[key] = processor;

            // Recurse for deep nesting
            if (embedded.EmbeddedTables != null && embedded.EmbeddedTables.Count > 0)
            {
                RegisterEmbeddedTables(rootConfig, embedded.EmbeddedTables, embedded.PrimaryKeyColumns, path);
            }
        }
    }

    public CdcSinkTableProcessor GetProcessor(string processorKey)
    {
        if (_tableIndex.TryGetValue(processorKey, out var processor) == false)
            throw new InvalidOperationException($"No processor found for table key '{processorKey}'.");
        return processor;
    }

    public CdcSinkDocumentOp ProcessRow(CdcSinkRow row)
    {
        var key = MakeKey(row.TableSchema, row.TableName);

        if (_tableIndex.TryGetValue(key, out var processor) == false)
            throw new InvalidOperationException($"Received CDC row for table '{key}' which is not configured in the CDC Sink task.");

        if (processor.IsRoot)
            return ProcessRootRow(row, processor);

        return ProcessEmbeddedRow(row, processor);
    }

    private CdcSinkDocumentOp ProcessRootRow(CdcSinkRow row, CdcSinkTableProcessor processor)
    {
        var config = processor.RootConfig;
        var documentId = processor.GenerateDocumentId(row.Data, config.PrimaryKeyColumns);

        if (documentId == null)
            throw new InvalidOperationException(
                $"Cannot generate document ID for table '{config.SourceTableSchema}.{config.SourceTableName}': " +
                $"one or more primary key columns ({string.Join(", ", config.PrimaryKeyColumns)}) are missing or null in the CDC row.");

        if (row.Operation == CdcSinkOperation.Delete)
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
                RawData = row.Data,
            };
        }

        var mappedData = processor.MapColumns(row.Data, config.ColumnsMapping);
        processor.ApplyLinks(mappedData, row.Data);

        mappedData[Constants.Documents.Metadata.Key] = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Collection] = config.Name,
        };

        return new CdcSinkDocumentOp
        {
            Type = CdcSinkDocumentOpType.Put,
            DocumentId = documentId,
            Processor = processor,
            MappedData = mappedData,
            RawData = row.Data,
            Operation = CdcSinkOperation.Upsert,
        };
    }

    private CdcSinkDocumentOp ProcessEmbeddedRow(CdcSinkRow row, CdcSinkTableProcessor processor)
    {
        var onDelete = processor.EmbeddedConfig.OnDelete;
        if (row.Operation == CdcSinkOperation.Delete && onDelete?.IgnoreDeletes == true && onDelete.Patch == null)
            return null; // silently ignore — no patch, no delete

        var parentDocumentId = processor.GetParentDocumentId(row.Data);

        if (parentDocumentId == null)
            throw new InvalidOperationException(
                $"Cannot determine parent document ID for embedded table '{processor.EmbeddedConfig.SourceTableSchema}.{processor.EmbeddedConfig.SourceTableName}': " +
                $"root join columns ({string.Join(", ", processor.RootJoinColumns)}) are missing or null in the CDC row.");
        var mappedData = processor.MapColumns(row.Data, processor.EmbeddedConfig.ColumnsMapping);

        return new CdcSinkDocumentOp
        {
            Type = CdcSinkDocumentOpType.EmbeddedModify,
            DocumentId = parentDocumentId,
            Processor = processor,
            MappedData = mappedData,
            RawData = row.Data,
            Operation = row.Operation,
        };
    }

    private static string MakeKey(string schema, string tableName)
    {
        if (string.IsNullOrEmpty(schema))
            return tableName;
        return schema + "." + tableName;
    }
}
