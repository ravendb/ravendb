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

    /// <summary>
    /// PostgreSQL-specific settings (publication name, slot name).
    /// Null for SQL Server configurations. Auto-filled on creation if omitted.
    /// </summary>
    public CdcSinkPostgresSettings Postgres { get; set; }

    /// <summary>
    /// When true, the initial full-table load is skipped — tables are marked as
    /// loaded immediately and the task starts streaming CDC changes. Use this when
    /// the target RavenDB database is already populated (e.g., from a prior migration).
    /// </summary>
    public bool SkipInitialLoad { get; set; }

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
            if (string.IsNullOrWhiteSpace(table.CollectionName))
                errors.Add("Table collection name must not be empty");

            if (string.IsNullOrWhiteSpace(table.SourceTableName))
                errors.Add($"Table '{table.CollectionName}' must have a source table name");

            if (table.PrimaryKeyColumns == null || table.PrimaryKeyColumns.Count == 0)
                errors.Add($"Table '{table.CollectionName}' must have at least one primary key column");

            if (table.Columns == null || table.Columns.Count == 0)
                errors.Add($"Table '{table.CollectionName}' must have at least one column mapping");

            if (uniqueNames.Add(table.CollectionName) == false)
                errors.Add($"Table name '{table.CollectionName}' is already defined. Table names must be unique");

            ValidateColumnsAndPropertyNames(table.CollectionName, table.Columns, table.EmbeddedTables, table.LinkedTables, errors);
            ValidateEmbeddedTables(table.EmbeddedTables, table.CollectionName, errors);
            ValidateLinkedTables(table.LinkedTables, table.CollectionName, errors);
        }

        return errors.Count == 0;
    }

    private static void ValidateColumnsAndPropertyNames(string tableName,
        List<CdcColumnMapping> columns,
        List<CdcSinkEmbeddedTableConfig> embeddedTables,
        List<CdcSinkLinkedTableConfig> linkedTables,
        List<string> errors)
    {
        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var propertyNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (columns == null)
        {
            errors.Add($"Table '{tableName}': Columns list is null");
            return;
        }

        foreach (var col in columns)
            {
                if (string.IsNullOrWhiteSpace(col.Column))
                {
                    var nameHint = string.IsNullOrWhiteSpace(col.Name) ? "" : $" (Name: '{col.Name}')";
                    errors.Add($"Table '{tableName}': column mapping has an empty Column name{nameHint}");
                    continue;
                }

                if (string.IsNullOrWhiteSpace(col.Name))
                {
                    errors.Add($"Table '{tableName}': column '{col.Column}' has an empty Name");
                    continue;
                }

                if (columnNames.Add(col.Column) == false)
                    errors.Add($"Table '{tableName}': duplicate column '{col.Column}'");

                if (propertyNames.Add(col.Name) == false)
                    errors.Add($"Table '{tableName}': duplicate target name '{col.Name}' (used by multiple columns)");
        }

        if (embeddedTables != null)
        {
            foreach (var embedded in embeddedTables)
            {
                if (embedded.PropertyName != null && propertyNames.Add(embedded.PropertyName) == false)
                    errors.Add($"Table '{tableName}': property name '{embedded.PropertyName}' from embedded table '{embedded.SourceTableName}' conflicts with a column mapping or another embedded/linked table");
            }
        }

        if (linkedTables != null)
        {
            foreach (var linked in linkedTables)
            {
                if (linked.PropertyName != null && propertyNames.Add(linked.PropertyName) == false)
                    errors.Add($"Table '{tableName}': property name '{linked.PropertyName}' from linked table '{linked.SourceTableName}' conflicts with a column mapping or another embedded/linked table");
            }
        }
    }

    private static void ValidateEmbeddedTables(List<CdcSinkEmbeddedTableConfig> embeddedTables, string parentName, List<string> errors)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (embeddedTables == null)
            return;

        var propertyNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var embedded in embeddedTables)
        {
            if (string.IsNullOrWhiteSpace(embedded.SourceTableName))
                errors.Add($"Embedded table under '{parentName}' must have a source table name");
            else if (string.Equals(embedded.SourceTableName, parentName, StringComparison.OrdinalIgnoreCase))
                errors.Add($"Embedded table '{embedded.SourceTableName}' under '{parentName}' cannot reference its own parent table");

            if (string.IsNullOrWhiteSpace(embedded.PropertyName))
                errors.Add($"Embedded table '{embedded.SourceTableName}' under '{parentName}' must have a property name");
            else if (propertyNames.Add(embedded.PropertyName) == false)
                errors.Add($"Embedded table property name '{embedded.PropertyName}' under '{parentName}' is already defined. Property names must be unique within the same parent");

            if (embedded.JoinColumns == null || embedded.JoinColumns.Count == 0)
                errors.Add($"Embedded table '{embedded.SourceTableName}' under '{parentName}' must have join columns");

            if (embedded.PrimaryKeyColumns == null || embedded.PrimaryKeyColumns.Count == 0)
                errors.Add($"Embedded table '{embedded.SourceTableName}' under '{parentName}' must have primary key columns");

            if (embedded.Columns == null || embedded.Columns.Count == 0)
                errors.Add($"Embedded table '{embedded.SourceTableName}' under '{parentName}' must have at least one column mapping");

            ValidateColumnsAndPropertyNames(embedded.SourceTableName, embedded.Columns, embedded.EmbeddedTables, linkedTables: null, errors);

            ValidateEmbeddedTables(embedded.EmbeddedTables, embedded.SourceTableName, errors);
        }
    }

    private static void ValidateLinkedTables(List<CdcSinkLinkedTableConfig> linkedTables, string parentName, List<string> errors)
    {
        if (linkedTables == null)
            return;

        var propertyNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var linked in linkedTables)
        {
            if (string.IsNullOrWhiteSpace(linked.SourceTableName))
                errors.Add($"Linked table under '{parentName}' must have a source table name");

            if (string.IsNullOrWhiteSpace(linked.PropertyName))
                errors.Add($"Linked table '{linked.SourceTableName}' under '{parentName}' must have a property name");
            else if (propertyNames.Add(linked.PropertyName) == false)
                errors.Add($"Linked table property name '{linked.PropertyName}' under '{parentName}' is already defined. Property names must be unique within the same parent");

            if (string.IsNullOrWhiteSpace(linked.LinkedCollectionName))
                errors.Add($"Linked table '{linked.SourceTableName}' under '{parentName}' must have a linked collection name");

            if (linked.JoinColumns == null || linked.JoinColumns.Count == 0)
                errors.Add($"Linked table '{linked.SourceTableName}' under '{parentName}' must have join columns");

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
            [nameof(Postgres)] = Postgres?.ToJson(),
            [nameof(SkipInitialLoad)] = SkipInitialLoad,
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
        localTables.Sort((a, b) => string.Compare(a.CollectionName, b.CollectionName, StringComparison.OrdinalIgnoreCase));
        var remoteTables = new List<CdcSinkTableConfig>(config.Tables);
        remoteTables.Sort((a, b) => string.Compare(a.CollectionName, b.CollectionName, StringComparison.OrdinalIgnoreCase));

        var count = localTables.Count < remoteTables.Count ? localTables.Count : remoteTables.Count;
        for (int i = 0; i < count; i++)
        {
            var local = localTables[i];
            var remote = remoteTables[i];

            if (string.Equals(local.CollectionName, remote.CollectionName, StringComparison.OrdinalIgnoreCase) == false)
            {
                differences |= CdcSinkConfigurationCompareDifferences.TableName;
                tableDiffs?.Add((local.CollectionName, CdcSinkConfigurationCompareDifferences.TableName));
            }

            if (local.Disabled != remote.Disabled)
            {
                differences |= CdcSinkConfigurationCompareDifferences.TableDisabled;
                tableDiffs?.Add((local.CollectionName, CdcSinkConfigurationCompareDifferences.TableDisabled));
            }

            if (HasTableConfigChanged(local, remote))
            {
                differences |= CdcSinkConfigurationCompareDifferences.TableConfig;
                tableDiffs?.Add((local.CollectionName, CdcSinkConfigurationCompareDifferences.TableConfig));
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
        if (string.Equals(local.SourceTableSchema, remote.SourceTableSchema, StringComparison.OrdinalIgnoreCase) == false)
            return true;

        if (string.Equals(local.SourceTableName, remote.SourceTableName, StringComparison.OrdinalIgnoreCase) == false)
            return true;

        if (local.Patch != remote.Patch)
            return true;

        if (HasOnDeleteChanged(local.OnDelete, remote.OnDelete))
            return true;

        if (HaveColumnsChanged(local.Columns, remote.Columns))
            return true;

        if ((local.PrimaryKeyColumns?.SequenceEqual(remote.PrimaryKeyColumns ?? []) ?? remote.PrimaryKeyColumns == null) == false)
            return true;

        if (HaveEmbeddedTablesChanged(local.EmbeddedTables, remote.EmbeddedTables))
            return true;

        if (HaveLinkedTablesChanged(local.LinkedTables, remote.LinkedTables))
            return true;

        return false;
    }

    private static bool HasOnDeleteChanged(CdcSinkOnDeleteConfig local, CdcSinkOnDeleteConfig remote)
    {
        if (local == null && remote == null)
            return false;
        if (local == null || remote == null)
            return true;
        return local.Patch != remote.Patch || local.IgnoreDeletes != remote.IgnoreDeletes;
    }

    private static bool HaveEmbeddedTablesChanged(List<CdcSinkEmbeddedTableConfig> local, List<CdcSinkEmbeddedTableConfig> remote)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if ((local?.Count ?? 0) != (remote?.Count ?? 0))
            return true;

        if (local == null)
            return false;

        // Sort copies by PropertyName for stable comparison regardless of order
        var sortedLocal = new List<CdcSinkEmbeddedTableConfig>(local);
        sortedLocal.Sort((a, b) => string.Compare(a.PropertyName, b.PropertyName, StringComparison.OrdinalIgnoreCase));
        var sortedRemote = new List<CdcSinkEmbeddedTableConfig>(remote);
        sortedRemote.Sort((a, b) => string.Compare(a.PropertyName, b.PropertyName, StringComparison.OrdinalIgnoreCase));

        for (int i = 0; i < sortedLocal.Count; i++)
        {
            var l = sortedLocal[i];
            var r = sortedRemote[i];

            if (string.Equals(l.SourceTableSchema, r.SourceTableSchema, StringComparison.OrdinalIgnoreCase) == false ||
                string.Equals(l.SourceTableName, r.SourceTableName, StringComparison.OrdinalIgnoreCase) == false ||
                string.Equals(l.PropertyName, r.PropertyName, StringComparison.OrdinalIgnoreCase) == false ||
                l.Patch != r.Patch ||
                l.Type != r.Type ||
                l.CaseSensitiveKeys != r.CaseSensitiveKeys)
                return true;

            if (HasOnDeleteChanged(l.OnDelete, r.OnDelete))
                return true;

            if (l.PrimaryKeyColumns.SequenceEqual(r.PrimaryKeyColumns) == false)
                return true;

            if (l.JoinColumns.SequenceEqual(r.JoinColumns) == false)
                return true;

            if (HaveColumnsChanged(l.Columns, r.Columns))
                return true;

            if (HaveEmbeddedTablesChanged(l.EmbeddedTables, r.EmbeddedTables))
                return true;
        }

        return false;
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
            ForEachEmbeddedTable(table.EmbeddedTables, e =>
                tables.Add(new TableInfo
                {
                    Schema = e.SourceTableSchema ?? defaultSchema,
                    TableName = e.SourceTableName,
                    PrimaryKeyColumns = e.PrimaryKeyColumns,
                }));
        }
        return tables;
    }

    /// <summary>
    /// Walks every embedded table in the configuration tree (depth-first, recursive).
    /// A single traversal with a stack guard — all callers that need to visit embedded
    /// tables should use this instead of rolling their own recursion.
    /// </summary>
    public static void ForEachEmbeddedTable(List<CdcSinkEmbeddedTableConfig> embedded, Action<CdcSinkEmbeddedTableConfig> action)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (embedded == null)
            return;

        foreach (var e in embedded)
        {
            action(e);
            ForEachEmbeddedTable(e.EmbeddedTables, action);
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

    private static bool HaveColumnsChanged(List<CdcColumnMapping> local, List<CdcColumnMapping> remote)
    {
        if ((local?.Count ?? 0) != (remote?.Count ?? 0))
            return true;

        if (local == null)
            return false;

        var sortedLocal = local.OrderBy(x => x.Column, StringComparer.OrdinalIgnoreCase).ToList();
        var sortedRemote = remote.OrderBy(x => x.Column, StringComparer.OrdinalIgnoreCase).ToList();

        for (int i = 0; i < sortedLocal.Count; i++)
        {
            var l = sortedLocal[i];
            var r = sortedRemote[i];

            if (string.Equals(l.Column, r.Column, StringComparison.OrdinalIgnoreCase) == false ||
                string.Equals(l.Name, r.Name, StringComparison.OrdinalIgnoreCase) == false ||
                l.Type != r.Type)
                return true;
        }

        return false;
    }

    private static bool HaveLinkedTablesChanged(List<CdcSinkLinkedTableConfig> local, List<CdcSinkLinkedTableConfig> remote)
    {
        if ((local?.Count ?? 0) != (remote?.Count ?? 0))
            return true;

        if (local == null)
            return false;

        // Sort copies by PropertyName for stable comparison regardless of order
        var sortedLocal = new List<CdcSinkLinkedTableConfig>(local);
        sortedLocal.Sort((a, b) => string.Compare(a.PropertyName, b.PropertyName, StringComparison.OrdinalIgnoreCase));
        var sortedRemote = new List<CdcSinkLinkedTableConfig>(remote);
        sortedRemote.Sort((a, b) => string.Compare(a.PropertyName, b.PropertyName, StringComparison.OrdinalIgnoreCase));

        for (int i = 0; i < sortedLocal.Count; i++)
        {
            var l = sortedLocal[i];
            var r = sortedRemote[i];

            if (string.Equals(l.SourceTableSchema, r.SourceTableSchema, StringComparison.OrdinalIgnoreCase) == false ||
                string.Equals(l.SourceTableName, r.SourceTableName, StringComparison.OrdinalIgnoreCase) == false ||
                string.Equals(l.PropertyName, r.PropertyName, StringComparison.OrdinalIgnoreCase) == false ||
                string.Equals(l.LinkedCollectionName, r.LinkedCollectionName, StringComparison.OrdinalIgnoreCase) == false)
                return true;

            if (l.JoinColumns.SequenceEqual(r.JoinColumns) == false)
                return true;
        }

        return false;
    }
}
