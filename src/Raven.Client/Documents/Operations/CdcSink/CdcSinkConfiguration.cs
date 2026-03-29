using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

public class CdcSinkConfiguration : IDynamicJson, IDatabaseTask
{
    private bool _initialized;

    public long TaskId { get; set; }

    public bool Disabled { get; set; }

    public string Name { get; set; }

    public string MentorNode { get; set; }

    public bool PinToMentorNode { get; set; }

    public string ConnectionStringName { get; set; }

    internal bool TestMode { get; set; }

    public List<CdcSinkTableConfig> Tables { get; set; } = new();

    [JsonDeserializationIgnore]
    [JsonIgnore]
    internal SqlConnectionString Connection { get; set; }

    public void Initialize(SqlConnectionString connectionString)
    {
        Connection = connectionString;
        _initialized = true;
    }

    public virtual bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true)
    {
        if (validateConnection && _initialized == false)
            throw new InvalidOperationException("CDC Sink configuration must be initialized");

        errors = new List<string>();

        if (validateName && string.IsNullOrEmpty(Name))
            errors.Add($"{nameof(Name)} of CDC Sink configuration cannot be empty");

        if (TestMode == false && string.IsNullOrEmpty(ConnectionStringName))
            errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

        if (validateConnection && TestMode == false)
            Connection.Validate(errors);

        if (Tables.Count == 0)
            errors.Add($"'{nameof(Tables)}' list cannot be empty.");

        var uniqueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var table in Tables)
        {
            if (string.IsNullOrWhiteSpace(table.Name))
                errors.Add("Table collection name must not be empty");

            if (string.IsNullOrWhiteSpace(table.SourceTableName))
                errors.Add($"Table '{table.Name}' must have a source table name");

            if (table.PrimaryKeyColumns == null || table.PrimaryKeyColumns.Count == 0)
                errors.Add($"Table '{table.Name}' must have at least one primary key column");

            if (table.ColumnsMapping == null || table.ColumnsMapping.Count == 0)
                errors.Add($"Table '{table.Name}' must have at least one column mapping");

            if (uniqueNames.Add(table.Name) == false)
                errors.Add($"Table name '{table.Name}' is already defined. Table names must be unique");

            ValidateEmbeddedTables(table.EmbeddedTables, table.Name, errors);
        }

        return errors.Count == 0;
    }

    private static void ValidateEmbeddedTables(List<CdcSinkEmbeddedTableConfig> embeddedTables, string parentName, List<string> errors)
    {
        if (embeddedTables == null)
            return;

        foreach (var embedded in embeddedTables)
        {
            if (string.IsNullOrWhiteSpace(embedded.SourceTableName))
                errors.Add($"Embedded table under '{parentName}' must have a source table name");

            if (string.IsNullOrWhiteSpace(embedded.PropertyName))
                errors.Add($"Embedded table '{embedded.SourceTableName}' under '{parentName}' must have a property name");

            if (embedded.JoinColumns == null || embedded.JoinColumns.Count == 0)
                errors.Add($"Embedded table '{embedded.SourceTableName}' under '{parentName}' must have join columns");

            if (embedded.PrimaryKeyColumns == null || embedded.PrimaryKeyColumns.Count == 0)
                errors.Add($"Embedded table '{embedded.SourceTableName}' under '{parentName}' must have primary key columns");

            ValidateEmbeddedTables(embedded.EmbeddedTables, embedded.SourceTableName, errors);
        }
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(TaskId)] = TaskId,
            [nameof(Disabled)] = Disabled,
            [nameof(ConnectionStringName)] = ConnectionStringName,
            [nameof(MentorNode)] = MentorNode,
            [nameof(PinToMentorNode)] = PinToMentorNode,
            [nameof(Tables)] = new DynamicJsonArray(Tables.Select(x => x.ToJson())),
        };
    }

    public string GetDestination()
    {
        return Connection?.ConnectionString;
    }

    public ulong GetTaskKey()
    {
        Debug.Assert(TaskId != 0);
        return (ulong)TaskId;
    }

    public string GetMentorNode() => MentorNode;

    public string GetDefaultTaskName() => $"CDC Sink to {ConnectionStringName}";

    public string GetTaskName() => Name;

    public bool IsResourceIntensive() => false;

    public bool IsPinnedToMentorNode() => PinToMentorNode;

    internal CdcSinkConfigurationCompareDifferences Compare(
        CdcSinkConfiguration config,
        Dictionary<string, SqlConnectionString> connectionStrings,
        List<(string TableName, CdcSinkConfigurationCompareDifferences Difference)> tableDiffs = null)
    {
        if (config == null)
            throw new ArgumentNullException(nameof(config), "Got null config to compare");

        var differences = CdcSinkConfigurationCompareDifferences.None;

        if (config.Tables.Count != Tables.Count)
            differences |= CdcSinkConfigurationCompareDifferences.TablesCount;

        // Sort copies by name for stable comparison
        var localTables = new List<CdcSinkTableConfig>(Tables);
        localTables.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase));
        var remoteTables = new List<CdcSinkTableConfig>(config.Tables);
        remoteTables.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.OrdinalIgnoreCase));

        var count = localTables.Count < remoteTables.Count ? localTables.Count : remoteTables.Count;
        for (int i = 0; i < count; i++)
        {
            var local = localTables[i];
            var remote = remoteTables[i];

            if (string.Equals(local.Name, remote.Name, StringComparison.OrdinalIgnoreCase) == false)
            {
                differences |= CdcSinkConfigurationCompareDifferences.TableName;
                tableDiffs?.Add((local.Name, CdcSinkConfigurationCompareDifferences.TableName));
            }

            if (local.Disabled != remote.Disabled)
            {
                differences |= CdcSinkConfigurationCompareDifferences.TableDisabled;
                tableDiffs?.Add((local.Name, CdcSinkConfigurationCompareDifferences.TableDisabled));
            }

            if (HasTableConfigChanged(local, remote))
            {
                differences |= CdcSinkConfigurationCompareDifferences.TableConfig;
                tableDiffs?.Add((local.Name, CdcSinkConfigurationCompareDifferences.TableConfig));
            }
        }

        if (config.ConnectionStringName != ConnectionStringName)
            differences |= CdcSinkConfigurationCompareDifferences.ConnectionStringName;
        else if (config.ConnectionStringName != null)
        {
            var oldConnectionString = Connection;
            SqlConnectionString newConnectionString = null;
            connectionStrings?.TryGetValue(config.ConnectionStringName, out newConnectionString);

            if (newConnectionString == null || oldConnectionString.IsEqual(newConnectionString) == false)
                differences |= CdcSinkConfigurationCompareDifferences.ConnectionString;
        }

        if (string.Equals(config.Name, Name, StringComparison.OrdinalIgnoreCase) == false)
            differences |= CdcSinkConfigurationCompareDifferences.ConfigurationName;

        if (config.MentorNode != MentorNode)
            differences |= CdcSinkConfigurationCompareDifferences.MentorNode;

        if (config.Disabled != Disabled)
            differences |= CdcSinkConfigurationCompareDifferences.ConfigurationDisabled;

        return differences;
    }

    private static bool HasTableConfigChanged(CdcSinkTableConfig local, CdcSinkTableConfig remote)
    {
        if (local.SourceTableSchema != remote.SourceTableSchema)
            return true;

        if (local.SourceTableName != remote.SourceTableName)
            return true;

        if (local.Patch != remote.Patch)
            return true;

        if (local.ColumnsMapping.Count != remote.ColumnsMapping.Count ||
            local.ColumnsMapping.Any(kvp => remote.ColumnsMapping.TryGetValue(kvp.Key, out var v) == false || v != kvp.Value))
            return true;

        if (local.AttachmentNameMapping.Count != remote.AttachmentNameMapping.Count ||
            local.AttachmentNameMapping.Any(kvp => remote.AttachmentNameMapping.TryGetValue(kvp.Key, out var v) == false || v != kvp.Value))
            return true;

        if (local.PrimaryKeyColumns.SequenceEqual(remote.PrimaryKeyColumns) == false)
            return true;

        if (HaveEmbeddedTablesChanged(local.EmbeddedTables, remote.EmbeddedTables))
            return true;

        if (HaveLinkedTablesChanged(local.LinkedTables, remote.LinkedTables))
            return true;

        return false;
    }

    private static bool HaveEmbeddedTablesChanged(List<CdcSinkEmbeddedTableConfig> local, List<CdcSinkEmbeddedTableConfig> remote)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        
        if ((local?.Count ?? 0) != (remote?.Count ?? 0))
            return true;

        if (local == null)
            return false;

        for (int i = 0; i < local.Count; i++)
        {
            var l = local[i];
            var r = remote[i];

            if (l.SourceTableSchema != r.SourceTableSchema ||
                l.SourceTableName != r.SourceTableName ||
                l.PropertyName != r.PropertyName ||
                l.Patch != r.Patch ||
                l.Type != r.Type ||
                l.CaseSensitiveKeys != r.CaseSensitiveKeys)
                return true;

            if (l.PrimaryKeyColumns.SequenceEqual(r.PrimaryKeyColumns) == false)
                return true;

            if (l.JoinColumns.SequenceEqual(r.JoinColumns) == false)
                return true;

            if (l.ColumnsMapping.Count != r.ColumnsMapping.Count ||
                l.ColumnsMapping.Any(kvp => r.ColumnsMapping.TryGetValue(kvp.Key, out var v) == false || v != kvp.Value))
                return true;

            if (HaveEmbeddedTablesChanged(l.EmbeddedTables, r.EmbeddedTables))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Collects fully-qualified source table names (schema.table) from all configured tables,
    /// including embedded tables recursively.
    /// </summary>
    /// <param name="defaultSchema">Default schema when SourceTableSchema is null (e.g., "public" for PostgreSQL, "dbo" for SQL Server).</param>
    public List<string> CollectAllSourceTableNames(string defaultSchema)
    {
        var names = new List<string>();
        foreach (var table in Tables)
        {
            var schema = table.SourceTableSchema ?? defaultSchema;
            names.Add($"{schema}.{table.SourceTableName}");
            CollectEmbeddedSourceTableNames(table.EmbeddedTables, defaultSchema, names);
        }
        return names;

        static void CollectEmbeddedSourceTableNames(List<CdcSinkEmbeddedTableConfig> embedded, string defaultSchema, List<string> names)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (embedded == null)
                return;

            foreach (var e in embedded)
            {
                var schema = e.SourceTableSchema ?? defaultSchema;
                names.Add($"{schema}.{e.SourceTableName}");
                CollectEmbeddedSourceTableNames(e.EmbeddedTables, defaultSchema, names);
            }
        }
    }

    /// <summary>
    /// Collects all configured tables (including embedded tables recursively) as a flat list
    /// of TableInfo instances with schema, name, and primary key columns.
    /// </summary>
    /// <param name="defaultSchema">Default schema when SourceTableSchema is null (e.g., "public" for PostgreSQL, "dbo" for SQL Server).</param>
    public List<TableInfo> CollectAllTablesFlat(string defaultSchema)
    {
        var tables = new List<TableInfo>();
        foreach (var table in Tables)
        {
            tables.Add(new TableInfo
            {
                Schema = table.SourceTableSchema ?? defaultSchema,
                TableName = table.SourceTableName,
                PrimaryKeyColumns = table.PrimaryKeyColumns,
            });
            CollectEmbeddedTablesFlat(table.EmbeddedTables, defaultSchema, tables);
        }
        return tables;

        static void CollectEmbeddedTablesFlat(List<CdcSinkEmbeddedTableConfig> embedded, string defaultSchema, List<TableInfo> tables)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();

            if (embedded == null)
                return;

            foreach (var e in embedded)
            {
                tables.Add(new TableInfo
                {
                    Schema = e.SourceTableSchema ?? defaultSchema,
                    TableName = e.SourceTableName,
                    PrimaryKeyColumns = e.PrimaryKeyColumns,
                });
                CollectEmbeddedTablesFlat(e.EmbeddedTables, defaultSchema, tables);
            }
        }
    }

    public class TableInfo
    {
        public string Schema { get; set; }
        public string TableName { get; set; }
        public List<string> PrimaryKeyColumns { get; set; }
        public string FullName => $"{Schema}.{TableName}";

        public override int GetHashCode() => StringComparer.OrdinalIgnoreCase.GetHashCode(FullName);
        public override bool Equals(object obj) => obj is TableInfo other && string.Equals(FullName, other.FullName, StringComparison.OrdinalIgnoreCase);
    }

    private static bool HaveLinkedTablesChanged(List<CdcSinkLinkedTableConfig> local, List<CdcSinkLinkedTableConfig> remote)
    {
        if ((local?.Count ?? 0) != (remote?.Count ?? 0))
            return true;

        if (local == null)
            return false;

        for (int i = 0; i < local.Count; i++)
        {
            var l = local[i];
            var r = remote[i];

            if (l.SourceTableSchema != r.SourceTableSchema ||
                l.SourceTableName != r.SourceTableName ||
                l.PropertyName != r.PropertyName ||
                l.LinkedCollectionName != r.LinkedCollectionName ||
                l.Type != r.Type)
                return true;

            if (l.JoinColumns.SequenceEqual(r.JoinColumns) == false)
                return true;
        }

        return false;
    }
}
