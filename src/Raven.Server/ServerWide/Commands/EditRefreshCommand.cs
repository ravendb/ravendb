using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditRefreshCommand : UpdateDatabaseCommand
    {
        public RefreshConfiguration Configuration;
        public void UpdateDatabaseRecord(DatabaseRecord databaseRecord)
        {
            databaseRecord.Refresh = Configuration;
        }

        public EditRefreshCommand()
        {
        }

        public EditRefreshCommand(RefreshConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            record.Refresh = Configuration;
            record.ClusterState.LastRefreshIndex = index;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
