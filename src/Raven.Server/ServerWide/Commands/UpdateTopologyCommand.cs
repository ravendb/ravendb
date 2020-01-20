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

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Topology = Topology;
            if (record.IsSharded == false)
            {
                SetLeaderStampForTopology(record.Topology, etag);
            }
            else
            {
                for (var i = 0; i < record.Shards.Length; i++)
                {
                    SetLeaderStampForTopology(record.Shards[i], etag);
                }
            }
            return null;
        }

        private static void SetLeaderStampForTopology(DatabaseTopology topology, long etag)
        {
            if (topology.Stamp == null)
            {
                topology.Stamp = new LeaderStamp {Term = -1, LeadersTicks = -1, Index = -1};
            }

            topology.Stamp.Index = etag;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Topology)] = Topology.ToJson();
            json[nameof(RaftCommandIndex)] = RaftCommandIndex;
        }
    }
}
