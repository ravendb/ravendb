using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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

        var localTables = Tables.OrderBy(x => x.Name);
        var remoteTables = config.Tables.OrderBy(x => x.Name);

        using var localEnum = localTables.GetEnumerator();
        using var remoteEnum = remoteTables.GetEnumerator();

        while (localEnum.MoveNext() && remoteEnum.MoveNext())
        {
            var local = localEnum.Current;
            var remote = remoteEnum.Current;

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
        if (string.Equals(local.SourceTableSchema, remote.SourceTableSchema, StringComparison.OrdinalIgnoreCase) == false)
            return true;

        if (string.Equals(local.SourceTableName, remote.SourceTableName, StringComparison.OrdinalIgnoreCase) == false)
            return true;

        if (string.Equals(local.Patch, remote.Patch, StringComparison.Ordinal) == false)
            return true;

        if (DictionariesEqual(local.ColumnsMapping, remote.ColumnsMapping) == false)
            return true;

        if (DictionariesEqual(local.AttachmentNameMapping, remote.AttachmentNameMapping) == false)
            return true;

        if (ListsEqual(local.PrimaryKeyColumns, remote.PrimaryKeyColumns) == false)
            return true;

        // Compare embedded and linked tables by count — detailed comparison would require
        // recursive structural comparison. Count change is sufficient to trigger reload.
        if ((local.EmbeddedTables?.Count ?? 0) != (remote.EmbeddedTables?.Count ?? 0))
            return true;

        if ((local.LinkedTables?.Count ?? 0) != (remote.LinkedTables?.Count ?? 0))
            return true;

        return false;
    }

    private static bool DictionariesEqual(Dictionary<string, string> a, Dictionary<string, string> b)
    {
        if (a == null && b == null)
            return true;
        if (a == null || b == null)
            return false;
        if (a.Count != b.Count)
            return false;

        foreach (var kvp in a)
        {
            if (b.TryGetValue(kvp.Key, out var value) == false || value != kvp.Value)
                return false;
        }
        return true;
    }

    private static bool ListsEqual(List<string> a, List<string> b)
    {
        if (a == null && b == null)
            return true;
        if (a == null || b == null)
            return false;
        if (a.Count != b.Count)
            return false;

        for (int i = 0; i < a.Count; i++)
        {
            if (a[i] != b[i])
                return false;
        }
        return true;
    }
}
