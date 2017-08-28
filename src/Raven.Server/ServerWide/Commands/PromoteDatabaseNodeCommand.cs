using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PromoteDatabaseNodeCommand : UpdateDatabaseCommand
    {
        public string NodeTag;

        public PromoteDatabaseNodeCommand() : base(null)
        {

        }

        public PromoteDatabaseNodeCommand(string databaseName) : base(databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.Topology.Promotables.Contains(NodeTag) == false)
                return null;

            record.Topology.PromotablesStatus.Remove(NodeTag);
            record.Topology.DemotionReasons.Remove(NodeTag);
            record.Topology.Promotables.Remove(NodeTag);
            record.Topology.Members.Add(NodeTag);

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(NodeTag)] = NodeTag;
        }
    }
}
