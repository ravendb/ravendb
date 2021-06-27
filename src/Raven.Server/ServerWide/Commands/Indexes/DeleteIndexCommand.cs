using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class DeleteIndexCommand : UpdateDatabaseCommand
    {
        public string IndexName { get; set; }

        public DeleteIndexCommand()
        {
            // for deserialization
        }

        public DeleteIndexCommand(string name, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            IndexName = name;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            record.DeleteIndex(IndexName, index);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
        }
    }
}
