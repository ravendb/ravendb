using Raven.Client.Documents.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Integrations;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL.Commands
{
    public class EditPostgreSQLConfigurationCommand : UpdateDatabaseCommand
    {
        public PostgreSQLConfiguration Configuration;

        public EditPostgreSQLConfigurationCommand(
            PostgreSQLConfiguration configuration,
            string databaseName,
            string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.Integrations == null)
                record.Integrations = new IntegrationsConfiguration();

            record.Integrations.PostgreSQL = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
