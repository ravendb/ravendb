using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.CdcSink;

public sealed class RemoveCdcSinkProcessStateCommand : UpdateValueForDatabaseCommand
{
    public string ConfigurationName { get; set; }

    public string ScriptName { get; set; }

    public RemoveCdcSinkProcessStateCommand()
    {
        // for deserialization
    }

    public RemoveCdcSinkProcessStateCommand(string databaseName, string configurationName, string scriptName, string uniqueRequestId)
        : base(databaseName, uniqueRequestId)
    {
        ConfigurationName = configurationName;
        ScriptName = scriptName;
    }

    public override string GetItemId() =>
        CdcSinkProcessState.GenerateItemName(DatabaseName, ConfigurationName, ScriptName);

    protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject existingValue) =>
        new UpdatedValue(UpdatedValueActionType.Delete, value: null);

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(ConfigurationName)] = ConfigurationName;
        json[nameof(ScriptName)] = ScriptName;
    }
}
