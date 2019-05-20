using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class RemoveNodeFromDatabaseCommand : UpdateDatabaseCommand
    {
        public string NodeTag;

        public RemoveNodeFromDatabaseCommand()
        {
        }

        public RemoveNodeFromDatabaseCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.Topology.RemoveFromTopology(NodeTag);
            record.DeletionInProgress?.Remove(NodeTag);

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(RaftCommandIndex)] = RaftCommandIndex;
        }
    }
}
