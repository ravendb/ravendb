using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

public class CdcSinkScript
{
    public string Name { get; set; }

    public string Script { get; set; }

    public bool Disabled { get; set; }

    internal CdcSinkConfigurationCompareDifferences Compare(CdcSinkScript script)
    {
        if (script == null)
            throw new ArgumentNullException(nameof(script), "Got null transformation to compare");

        var differences = CdcSinkConfigurationCompareDifferences.None;

        if (script.Name.Equals(Name, StringComparison.OrdinalIgnoreCase) == false)
            differences |= CdcSinkConfigurationCompareDifferences.ScriptName;

        if (script.Script != Script)
            differences |= CdcSinkConfigurationCompareDifferences.Script;

        if (script.Disabled != Disabled)
            differences |= CdcSinkConfigurationCompareDifferences.ScriptDisabled;

        return differences;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Script)] = Script,
        };
    }
}
