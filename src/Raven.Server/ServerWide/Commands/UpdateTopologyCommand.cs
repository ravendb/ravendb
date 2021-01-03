using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateTopologyCommand : UpdateDatabaseCommand
    {
        public DatabaseTopology Topology;

        public UpdateTopologyCommand()
        {
            //
        }

        public UpdateTopologyCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Topology = Topology;
            SetLeaderStampForTopology(record.Topology, etag);
            if (record.IsSharded == false) 
                return;
            
            foreach (var shardTopology in record.Shards)
            {
                SetLeaderStampForTopology(shardTopology, etag);
            }
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
        }
    }
}
