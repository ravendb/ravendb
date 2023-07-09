using System;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateTopologyCommand : UpdateDatabaseCommand
    {
        public DatabaseTopology Topology;
        public DateTime At;
        public int? ShardNumber;

        public UpdateTopologyCommand()
        {
            //
        }

        public UpdateTopologyCommand(string databaseName, DateTime at, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            At = at;

            if (ShardHelper.TryGetShardNumberAndDatabaseName(DatabaseName, out DatabaseName, out var shard))
                ShardNumber = shard;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Topology.NodesModifiedAt = At;
            SetLeaderStampForTopology(Topology, etag);

            if (ShardNumber == null)
            {
                if (record.IsSharded)
                {
                    record.Sharding.Orchestrator.Topology.Update(Topology);
                    return;
                }

                record.Topology = Topology;
                return;
            }

            if (record.Sharding.Shards.ContainsKey(ShardNumber.Value) == false)
                throw new RachisApplyException($"The requested shard '{ShardNumber.Value}' doesn't exists in '{record.DatabaseName}'");

            record.Sharding.Shards[ShardNumber.Value] = Topology;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Topology)] = Topology.ToJson();
            json[nameof(RaftCommandIndex)] = RaftCommandIndex;
            json[nameof(At)] = At;
            json[nameof(ShardNumber)] = ShardNumber;
        }
    }
}
