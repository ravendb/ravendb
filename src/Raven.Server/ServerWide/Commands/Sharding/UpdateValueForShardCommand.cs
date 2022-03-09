using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding;

public abstract class UpdateValueForShardCommand : UpdateValueForDatabaseCommand
{
    protected UpdateValueForShardCommand() { }
    protected UpdateValueForShardCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
    {
    }

    public string ShardName;
    public Dictionary<string, string> CurrentChangeVectorPerShard;
    public Dictionary<string, string> PreviouslyChangeVectorPerShard;

    public override DynamicJsonValue ToJson(JsonOperationContext context)
    {
        var djv = base.ToJson(context);
        if (ShardName == null)
            return djv;

        djv[nameof(ShardName)] = ShardName;
        djv[nameof(CurrentChangeVectorPerShard)] = CurrentChangeVectorPerShard?.ToJson();
        djv[nameof(PreviouslyChangeVectorPerShard)] = PreviouslyChangeVectorPerShard?.ToJson();

        return djv;
    }
}
