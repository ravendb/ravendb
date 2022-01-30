using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding;

public abstract class UpdateValueForShardCommand : UpdateValueForDatabaseCommand
{
    protected UpdateValueForShardCommand() { }
    protected UpdateValueForShardCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
    {
    }

    public string ShardDbId;
    public string ShardName;
    public string ShardLocalChangeVector;

    public override DynamicJsonValue ToJson(JsonOperationContext context)
    {
        var djv = base.ToJson(context);
        if (ShardName == null)
            return djv;

        djv[nameof(ShardName)] = ShardName;
        djv[nameof(ShardDbId)] = ShardDbId;
        djv[nameof(ShardLocalChangeVector)] = ShardLocalChangeVector;

        return djv;
    }
}
