using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

public class CdcSinkProcessState : IDatabaseTaskStatus
{
    public string NodeTag { get; set; }

    public string ConfigurationName { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ConfigurationName)] = ConfigurationName,
            [nameof(NodeTag)] = NodeTag,
        };
    }

    public static string GenerateItemName(string databaseName, string configurationName)
    {
        return $"values/{databaseName}/cdcsink/{configurationName}";
    }
}
