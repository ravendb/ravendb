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

    public List<CdcSinkScript> Scripts { get; set; } = new();

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

        var uniqueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (Scripts.Count == 0)
            throw new InvalidOperationException($"'{nameof(Scripts)}' list cannot be empty.");

        foreach (var script in Scripts)
        {
            if (string.IsNullOrWhiteSpace(script.Script))
                errors.Add($"Script '{Name}' must not be empty");

            if (uniqueNames.Add(script.Name) == false)
                errors.Add($"Script name '{script.Name}' name is already defined. The script names need to be unique");
        }

        return errors.Count == 0;
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
            [nameof(Scripts)] = new DynamicJsonArray(Scripts.Select(x => x.ToJson())),
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
        List<(string TransformationName, CdcSinkConfigurationCompareDifferences Difference)> transformationDiffs = null)
    {
        if (config == null)
            throw new ArgumentNullException(nameof(config), "Got null config to compare");

        var differences = CdcSinkConfigurationCompareDifferences.None;

        if (config.Scripts.Count != Scripts.Count)
            differences |= CdcSinkConfigurationCompareDifferences.ScriptsCount;

        var localTransforms = Scripts.OrderBy(x => x.Name);
        var remoteTransforms = config.Scripts.OrderBy(x => x.Name);

        using var localEnum = localTransforms.GetEnumerator();
        using var remoteEnum = remoteTransforms.GetEnumerator();

        while (localEnum.MoveNext() && remoteEnum.MoveNext())
        {
            var diff = localEnum.Current.Compare(remoteEnum.Current);
            differences |= diff;

            if (diff != CdcSinkConfigurationCompareDifferences.None)
                transformationDiffs?.Add((localEnum.Current.Name, diff));
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

        if (config.Name.Equals(Name, StringComparison.OrdinalIgnoreCase) == false)
            differences |= CdcSinkConfigurationCompareDifferences.ConfigurationName;

        if (config.MentorNode != MentorNode)
            differences |= CdcSinkConfigurationCompareDifferences.MentorNode;

        if (config.Disabled != Disabled)
            differences |= CdcSinkConfigurationCompareDifferences.ConfigurationDisabled;

        return differences;
    }
}
