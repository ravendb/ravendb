using Raven.Client.Server;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class RemoveNodeFromDatabaseCommand : UpdateDatabaseCommand
    {
        public string NodeTag;

        public RemoveNodeFromDatabaseCommand() : base(null)
        {
        }

        public RemoveNodeFromDatabaseCommand(string databaseName) : base(databaseName)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Topology.RemoveFromTopology(NodeTag);
            record.DeletionInProgress.Remove(NodeTag);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(Etag)] = Etag;
        }
    }
}
