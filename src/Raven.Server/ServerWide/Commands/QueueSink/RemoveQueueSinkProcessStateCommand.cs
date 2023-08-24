using Raven.Client.Documents.Operations.QueueSink;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.QueueSink;

public sealed class RemoveQueueSinkProcessStateCommand : UpdateValueForDatabaseCommand
{
    public string ConfigurationName { get; set; }

    public string ScriptName { get; set; }

    public RemoveQueueSinkProcessStateCommand()
    {
        // for deserialization
    }

    public RemoveQueueSinkProcessStateCommand(string databaseName, string configurationName, string scriptName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
    {
        ConfigurationName = configurationName;
        ScriptName = scriptName;
    }

    public override string GetItemId()
    {
        return QueueSinkProcessState.GenerateItemName(DatabaseName, ConfigurationName, ScriptName);
    }

    protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context,
        BlittableJsonReaderObject existingValue)
    {
        return new UpdatedValue(UpdatedValueActionType.Delete, value: null);
    }

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(ConfigurationName)] = ConfigurationName;
        json[nameof(ScriptName)] = ScriptName;
    }
}
