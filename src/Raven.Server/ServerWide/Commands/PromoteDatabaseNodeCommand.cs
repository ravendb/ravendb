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
            if (record.Topology.Promotables.Contains(NodeTag) == false)
                return;

            record.Topology.PromotablesStatus.Remove(NodeTag);
            record.Topology.DemotionReasons.Remove(NodeTag);
            record.Topology.Promotables.Remove(NodeTag);
            record.Topology.Members.Add(NodeTag);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(NodeTag)] = NodeTag;
        }
    }
}
