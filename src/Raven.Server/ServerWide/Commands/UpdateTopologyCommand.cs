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
        public int? Shard;

        public UpdateTopologyCommand()
        {
            //
        }

        public UpdateTopologyCommand(string databaseName, DateTime at, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            At = at;

            if (ShardHelper.TryGetShardNumberAndDatabaseName(ref DatabaseName, out var shard))
                Shard = shard;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Topology.NodesModifiedAt = At;
            SetLeaderStampForTopology(Topology, etag);

            if (Shard == null)
            {
                if (record.IsSharded)
                    throw new RachisApplyException($"The request database '{record.DatabaseName}' is sharded, Shard number must be provided");

                record.Topology = Topology;
                return;
            }

            if (record.Shards.Length <= Shard)
                throw new RachisApplyException($"The request shard '{Shard}' doesn't exists in '{record.DatabaseName}'");

            record.Shards[Shard.Value] = Topology;
        }
        
        private static void SetLeaderStampForTopology(DatabaseTopology topology, long etag)
        {
            topology.Stamp ??= new LeaderStamp {Term = -1, LeadersTicks = -1, Index = -1};
            topology.Stamp.Index = etag;
        }


        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Topology)] = Topology.ToJson();
            json[nameof(RaftCommandIndex)] = RaftCommandIndex;
            json[nameof(At)] = At;
            json[nameof(Shard)] = Shard;
        }
    }
}
