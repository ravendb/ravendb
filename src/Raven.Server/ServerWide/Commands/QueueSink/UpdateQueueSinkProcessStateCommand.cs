using System.Linq;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.QueueSink;

public sealed class UpdateQueueSinkProcessStateCommand : UpdateValueForDatabaseCommand
{
    public UpdateQueueSinkProcessStateCommand()
    {
        // for deserialization
    }

    public UpdateQueueSinkProcessStateCommand(string databaseName, QueueSinkProcessState state, bool hasHighlyAvailableTasks, string uniqueRequestId) : base(databaseName, uniqueRequestId)
    {
        State = state;
        HasHighlyAvailableTasks = hasHighlyAvailableTasks;
    }

    public QueueSinkProcessState State { get; set; }

    public bool HasHighlyAvailableTasks { get; set; }

    public override string GetItemId()
    {
        var databaseName = ShardHelper.ToDatabaseName(DatabaseName);

        return QueueSinkProcessState.GenerateItemName(databaseName, State.ConfigurationName, State.ScriptName);
    }

    public override void FillJson(DynamicJsonValue json)
    {
        json[nameof(State)] = State.ToJson();
        json[nameof(HasHighlyAvailableTasks)] = HasHighlyAvailableTasks;
    }

    protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject existingValue)
    {
        if (existingValue != null)
        {
            var databaseTask = record.QueueSinks.FirstOrDefault(x => x.Name == State.ConfigurationName);

            if (databaseTask == null)
                throw new RachisApplyException($"Can't update state of Queue Sink '{State.ConfigurationName}' by node {State.NodeTag}, because its configuration can't be found");

            var topology = record.Topology;
            var lastResponsibleNode = GetLastResponsibleNode(HasHighlyAvailableTasks, topology, State.NodeTag);
            if (topology.WhoseTaskIsIt(RachisState.Follower, databaseTask, lastResponsibleNode) != State.NodeTag)
                throw new RachisApplyException($"Can't update state of Queue Sink {State.ConfigurationName} by node {State.NodeTag}, because it's not its task to update this Queue Sink");
        }

        return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(State.ToJson(), GetItemId()));
    }
}
