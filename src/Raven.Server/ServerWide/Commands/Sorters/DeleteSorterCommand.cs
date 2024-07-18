using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sorters
{
    public class DeleteSorterCommand : UpdateDatabaseCommand
    {
        public string SorterName;

        public DeleteSorterCommand()
        {
            // for deserialization
        }

        public DeleteSorterCommand(string name, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            SorterName = name;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeleteSorter(SorterName);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SorterName)] = SorterName;
        }

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
        }
    }
}
