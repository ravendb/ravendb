using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class DeleteIndexCommand : UpdateDatabaseCommand
    {
        public string IndexName { get; set; }

        public DeleteIndexCommand() : base(null)
        {
            // for deserialization
        }

        public DeleteIndexCommand(string name, string databaseName)
            : base(databaseName)
        {
            IndexName = name;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeleteIndex(IndexName);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
        }
    }
}