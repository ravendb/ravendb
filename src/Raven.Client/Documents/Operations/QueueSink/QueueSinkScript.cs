using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.QueueSink;

public class QueueSinkScript
{
    public string Name { get; set; }
    
    public List<string> Queues { get; set; } = new();

    public string Script { get; set; }
    
    public bool Disabled { get; set; }

    internal QueueSinkConfigurationCompareDifferences Compare(QueueSinkScript script)
    {
        if (script == null)
            throw new ArgumentNullException(nameof(script), "Got null transformation to compare");

        var differences = QueueSinkConfigurationCompareDifferences.None;

        if (script.Queues.Count != Queues.Count)
            differences |= QueueSinkConfigurationCompareDifferences.ScriptsCount;

        var queues = new List<string>(Queues);

        foreach (var queue in script.Queues)
        {
            queues.Remove(queue);
        }

        if (queues.Count != 0)
            differences |= QueueSinkConfigurationCompareDifferences.QueueCount;

        if (script.Name.Equals(Name, StringComparison.OrdinalIgnoreCase) == false)
            differences |= QueueSinkConfigurationCompareDifferences.ScriptName;

        if (script.Script != Script)
            differences |= QueueSinkConfigurationCompareDifferences.Script;

        if (script.Disabled != Disabled)
            differences |= QueueSinkConfigurationCompareDifferences.ScriptDisabled;

        return differences;
    }
    
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Script)] = Script,
            [nameof(Queues)] = Queues,
        };
    }
}
