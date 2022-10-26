using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PromoteDatabaseNodeCommand : UpdateDatabaseCommand
    {
        public string NodeTag;

        public PromoteDatabaseNodeCommand()
        {

        }

        public PromoteDatabaseNodeCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var topology = record.IsSharded ? record.Sharding.Orchestrator.Topology : record.Topology;

            if (topology.Promotables.Contains(NodeTag) == false)
                return ;

            topology.PromotablesStatus.Remove(NodeTag);
            topology.DemotionReasons.Remove(NodeTag);
            topology.Promotables.Remove(NodeTag);
            topology.Members.Add(NodeTag);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(NodeTag)] = NodeTag;
        }
    }
}
