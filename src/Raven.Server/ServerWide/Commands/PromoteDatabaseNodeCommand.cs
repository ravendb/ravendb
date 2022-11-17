using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PromoteDatabaseNodeCommand : UpdateDatabaseCommand
    {
        public string NodeTag;
        public int? Shard;

        public PromoteDatabaseNodeCommand()
        {
            
        }

        public PromoteDatabaseNodeCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            if (ShardHelper.TryGetShardNumberAndDatabaseName(ref DatabaseName, out var shard))
                Shard = shard;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            DatabaseTopology topology;
            if (Shard.HasValue == false)
            {
                topology = record.IsSharded ? record.Sharding.Orchestrator.Topology : record.Topology;
            }
            else
            {
                if (record.Sharding.Shards.Length <= Shard)
                    throw new RachisApplyException($"The request shard '{Shard}' doesn't exists in '{record.DatabaseName}'");

                topology = record.Sharding.Shards[Shard.Value];
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
            json[nameof(Shard)] = Shard;
        }
    }
}
