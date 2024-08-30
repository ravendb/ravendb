using System;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public sealed class CreateNewShardCommand : UpdateDatabaseCommand
    {
        public DatabaseTopology Topology;
        public DateTime At;
        public int ShardNumber;

        public CreateNewShardCommand()
        {
            //
        }

        public CreateNewShardCommand(string databaseName, int shardNumber, DatabaseTopology shardTopology, DateTime at, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            if (ShardHelper.IsShardName(databaseName))
                throw new ArgumentException($"The command {nameof(CreateNewShardCommand)} expected a sharded database name but got shard {databaseName} instead.");

            Topology = shardTopology;
            At = at;
            ShardNumber = shardNumber;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Topology.NodesModifiedAt = At;
            SetLeaderStampForTopology(Topology, etag);

            if (record.Sharding.Shards.ContainsKey(ShardNumber))
                throw new RachisApplyException($"Cannot add new shard {ShardNumber} to the database {DatabaseName} because it already exists.");
            record.Sharding.Shards.Add(ShardNumber, Topology);
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
