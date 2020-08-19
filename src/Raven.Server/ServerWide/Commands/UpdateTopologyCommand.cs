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
            if (record.Topology.Stamp == null)
            {
                record.Topology.Stamp = new LeaderStamp
                {
                    Term = -1,
                    LeadersTicks = -1,
                    Index = -1
                };
            }
            record.Topology.Stamp.Index = etag;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Topology)] = Topology.ToJson();
            json[nameof(RaftCommandIndex)] = RaftCommandIndex;
        }
    }
}
