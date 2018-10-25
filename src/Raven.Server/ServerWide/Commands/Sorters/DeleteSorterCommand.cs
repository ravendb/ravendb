using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sorters
{
    public class DeleteSorterCommand : UpdateDatabaseCommand
    {
        public string SorterName;

        public DeleteSorterCommand() : base(null)
        {
            // for deserialization
        }

        public DeleteSorterCommand(string name, string databaseName)
            : base(databaseName)
        {
            SorterName = name;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeleteSorter(SorterName);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SorterName)] = SorterName;
        }
    }
}
