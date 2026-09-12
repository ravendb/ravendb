using System.Linq;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.CdcSink;

public sealed class UpdateCdcSinkProcessStateCommand : UpdateValueForDatabaseCommand
{
    public UpdateCdcSinkProcessStateCommand()
    {
        // for deserialization
    }

    public UpdateCdcSinkProcessStateCommand(string databaseName, CdcSinkProcessState state, bool hasHighlyAvailableTasks, string uniqueRequestId)
        : base(databaseName, uniqueRequestId)
    {
        State = state;
        HasHighlyAvailableTasks = hasHighlyAvailableTasks;
    }

    public CdcSinkProcessState State { get; set; }

    public bool HasHighlyAvailableTasks { get; set; }

    public override string GetItemId()
    {
        var databaseName = ShardHelper.ToDatabaseName(DatabaseName);
        return CdcSinkProcessState.GenerateItemName(databaseName, State.ConfigurationName);
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
            var databaseTask = record.CdcSinks.FirstOrDefault(x => x.Name == State.ConfigurationName);

            if (databaseTask == null)
                throw new RachisApplyException($"Can't update state of CDC Sink '{State.ConfigurationName}' by node {State.NodeTag}, because its configuration can't be found");

            var topology = record.Topology;
            var lastResponsibleNode = GetLastResponsibleNode(HasHighlyAvailableTasks, topology, State.NodeTag);
            if (topology.WhoseTaskIsIt(RachisState.Follower, databaseTask, lastResponsibleNode) != State.NodeTag)
                throw new RachisApplyException($"Can't update state of CDC Sink {State.ConfigurationName} by node {State.NodeTag}, because it's not its task to update this CDC Sink");
        }

        return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(State.ToJson(), GetItemId()));
    }
}
