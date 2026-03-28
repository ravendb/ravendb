using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Operations.CdcSink;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Core CDC row routing and processing engine.
/// Stateless — takes configuration, builds lookup indexes, and processes individual rows
/// into document operations (put, delete, embedded modify).
/// </summary>
public class CdcSinkDocumentProcessor
{
    private readonly CdcSinkConfiguration _config;

    /// <summary>
    /// Lookup: "schema.tableName" (lowercase) → CdcSinkTableProcessor.
    /// </summary>
    private readonly Dictionary<string, CdcSinkTableProcessor> _tableIndex;

    public CdcSinkDocumentProcessor(CdcSinkConfiguration config)
    {
        _config = config;
        _tableIndex = new Dictionary<string, CdcSinkTableProcessor>(StringComparer.OrdinalIgnoreCase);

        foreach (var table in config.Tables)
        {
            // Register the root table
            var rootProcessor = new CdcSinkTableProcessor
            {
                RootConfig = table,
                CollectionName = table.Name,
                IsRoot = true,
            };

            var rootKey = MakeKey(table.SourceTableSchema, table.SourceTableName);
            _tableIndex[rootKey] = rootProcessor;

            // Register all embedded tables recursively
            if (table.EmbeddedTables != null)
            {
                RegisterEmbeddedTables(table, table.EmbeddedTables, table.PrimaryKeyColumns, new List<EmbeddedPathSegment>());
            }
        }
    }

    private void RegisterEmbeddedTables(
        CdcSinkTableConfig rootConfig,
        List<CdcSinkEmbeddedTableConfig> embeddedTables,
        List<string> parentPkColumns,
        List<EmbeddedPathSegment> currentPath)
    {
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

            var processor = new CdcSinkTableProcessor
            {
                RootConfig = rootConfig,
                CollectionName = rootConfig.Name,
                IsRoot = false,
                EmbeddedConfig = embedded,
                PathFromRoot = path,
                RootJoinColumns = rootJoinColumns,
            };

            var key = MakeKey(embedded.SourceTableSchema, embedded.SourceTableName);
            _tableIndex[key] = processor;

            // Recurse for deep nesting
            if (embedded.EmbeddedTables != null && embedded.EmbeddedTables.Count > 0)
            {
                RegisterEmbeddedTables(rootConfig, embedded.EmbeddedTables, embedded.PrimaryKeyColumns, path);
            }
        }
    }

    /// <summary>
    /// Process a single CDC row into a document operation.
    /// </summary>
    /// <returns>The operation to perform, or null if the table is not in the configuration.</returns>
    public CdcSinkDocumentOp ProcessRow(CdcSinkRow row)
    {
        var key = MakeKey(row.TableSchema, row.TableName);

        if (_tableIndex.TryGetValue(key, out var processor) == false)
            return null; // Unknown table, skip

        if (processor.IsRoot)
            return ProcessRootRow(row, processor);

        return ProcessEmbeddedRow(row, processor);
    }

    private CdcSinkDocumentOp ProcessRootRow(CdcSinkRow row, CdcSinkTableProcessor processor)
    {
        var config = processor.RootConfig;
        var documentId = processor.GenerateDocumentId(row.Data, config.PrimaryKeyColumns);

        if (documentId == null)
            return null; // Can't generate ID without PK values

        if (row.Operation == CdcSinkOperation.Delete)
        {
            return new CdcSinkDocumentOp
            {
                Type = CdcSinkDocumentOpType.Delete,
                DocumentId = documentId,
                Processor = processor,
                Operation = CdcSinkOperation.Delete,
                RawData = row.Data,
            };
        }

        // Upsert: build the document from column mappings
        var mappedData = processor.MapColumns(row.Data, config.ColumnsMapping);

        // Apply linked table references (FK → document ID)
        processor.ApplyLinks(mappedData, row.Data);

        // Set metadata
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
        // Compute the parent (root) document ID from the row's FK values
        var parentDocumentId = processor.GetParentDocumentId(row.Data);

        if (parentDocumentId == null)
            return null; // Can't determine parent without FK values

        // Map columns for the embedded item
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
