using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class PromoteDatabaseNodeCommand : UpdateDatabaseCommand
    {
        public string NodeTag;
        public int? ShardNumber;

        public PromoteDatabaseNodeCommand()
        {
            
        }

        public PromoteDatabaseNodeCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            if (ShardHelper.TryGetShardNumberAndDatabaseName(DatabaseName, out DatabaseName, out var shard))
                ShardNumber = shard;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            DatabaseTopology topology;
            if (ShardNumber.HasValue == false)
            {
                topology = record.IsSharded ? record.Sharding.Orchestrator.Topology : record.Topology;
            }
            else
            {
                if (record.Sharding.Shards.ContainsKey(ShardNumber.Value) == false)
                    throw new RachisApplyException($"The requested shard '{ShardNumber.Value}' doesn't exists in '{record.DatabaseName}'");

                topology = record.Sharding.Shards[ShardNumber.Value];
            }

            if (topology.Promotables.Contains(NodeTag) == false)
                return;

            topology.PromotablesStatus.Remove(NodeTag);
            topology.DemotionReasons.Remove(NodeTag);
            topology.Promotables.Remove(NodeTag);
            topology.Members.Add(NodeTag);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(ShardNumber)] = ShardNumber;
        }
    }
}
