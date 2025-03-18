using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.QueueSink;

/// <summary>
/// The configuration for a queue sink task, which allows integrating with external queueing systems.
/// </summary>
public class QueueSinkConfiguration : IDynamicJsonValueConvertible, IDatabaseTask
{
    private bool _initialized;

    /// <summary>
    /// Specifies the type of queue broker being used.
    /// </summary>
    public QueueBrokerType BrokerType { get; set; }

    /// <summary>
    /// The unique identifier for the task.
    /// </summary>
    public long TaskId { get; set; }

    /// <summary>
    /// Indicates whether the queue sink task is disabled.
    /// </summary>
    public bool Disabled { get; set; }

    /// <summary>
    /// The name of the queue sink task.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// The mentor node assigned to this task, if specified.
    /// </summary>
    public string MentorNode { get; set; }

    /// <summary>
    /// Determines whether the task should be pinned to the mentor node.
    /// </summary>
    public bool PinToMentorNode { get; set; }

    /// <summary>
    /// The name of the connection string used to connect to the queue broker.
    /// </summary>
    public string ConnectionStringName { get; set; }

    /// <summary>
    /// Indicates whether the configuration is running in test mode.
    /// </summary>
    internal bool TestMode { get; set; }

    /// <summary>
    /// A list of user-defined scripts that process incoming queue messages and define how they should be stored in RavenDB.
    /// </summary>
    public List<QueueSinkScript> Scripts { get; set; } = new();

    [JsonDeserializationIgnore]
    [JsonIgnore]
    internal QueueConnectionString Connection { get; set; }

    public void Initialize(QueueConnectionString connectionString)
    {
        Connection = connectionString;
        _initialized = true;
    }

    public virtual bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true)
    {
        if (validateConnection && _initialized == false)
            throw new InvalidOperationException("Queue Sink configuration must be initialized");

        errors = new List<string>();

        if (validateName && string.IsNullOrEmpty(Name))
            errors.Add($"{nameof(Name)} of Queue Sink configuration cannot be empty");

        if (TestMode == false && string.IsNullOrEmpty(ConnectionStringName))
            errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

        if (validateConnection && TestMode == false)
            Connection.Validate(ref errors);

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

        if (Connection != null && BrokerType != Connection.BrokerType)
        {
            errors.Add("Broker type must be the same in the Queue Sink configuration and in Connection string.");
            return false;
        }

        return errors.Count == 0;
    }

    public DynamicJsonValue ToJson()
    {
        var result = new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(TaskId)] = TaskId,
            [nameof(Disabled)] = Disabled,
            [nameof(ConnectionStringName)] = ConnectionStringName,
            [nameof(MentorNode)] = MentorNode,
            [nameof(PinToMentorNode)] = PinToMentorNode,
            [nameof(Scripts)] = new DynamicJsonArray(Scripts.Select(x => x.ToJson())),
            [nameof(BrokerType)] = BrokerType
        };

        return result;
    }

    public string GetDestination()
    {
        return Connection.GetUrl();
    }

    public ulong GetTaskKey()
    {
        Debug.Assert(TaskId != 0);
        return (ulong)TaskId;
    }

    public string GetMentorNode()
    {
        return MentorNode;
    }

    public string GetDefaultTaskName()
    {
        return $"Queue Sink to {ConnectionStringName}";
    }

    public string GetTaskName()
    {
        return Name;
    }

    public bool IsResourceIntensive()
    {
        return false;
    }

    public bool IsPinnedToMentorNode()
    {
        return PinToMentorNode;
    }
    
    internal QueueSinkConfigurationCompareDifferences Compare(QueueSinkConfiguration config, List<(string TransformationName, QueueSinkConfigurationCompareDifferences Difference)> transformationDiffs = null)
    {
        if (config == null)
            throw new ArgumentNullException(nameof(config), "Got null config to compare");

        var differences = QueueSinkConfigurationCompareDifferences.None;

        if (config.Scripts.Count != Scripts.Count)
            differences |= QueueSinkConfigurationCompareDifferences.ScriptsCount;

        var localTransforms = Scripts.OrderBy(x => x.Name);
        var remoteTransforms = config.Scripts.OrderBy(x => x.Name);

        using (var localEnum = localTransforms.GetEnumerator())
        using (var remoteEnum = remoteTransforms.GetEnumerator())
        {
            while (localEnum.MoveNext() && remoteEnum.MoveNext())
            {
                var transformationDiff = localEnum.Current.Compare(remoteEnum.Current);
                differences |= transformationDiff;

                if (transformationDiff != QueueSinkConfigurationCompareDifferences.None)
                {
                    transformationDiffs?.Add((localEnum.Current.Name, transformationDiff));
                }
            }
        }

        if (config.ConnectionStringName != ConnectionStringName)
            differences |= QueueSinkConfigurationCompareDifferences.ConnectionStringName;

        if (config.Name.Equals(Name, StringComparison.OrdinalIgnoreCase) == false)
            differences |= QueueSinkConfigurationCompareDifferences.ConfigurationName;

        if (config.MentorNode != MentorNode)
            differences |= QueueSinkConfigurationCompareDifferences.MentorNode;

        if (config.Disabled != Disabled)
            differences |= QueueSinkConfigurationCompareDifferences.ConfigurationDisabled;

        return differences;
    }
}
