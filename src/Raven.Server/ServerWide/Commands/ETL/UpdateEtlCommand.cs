using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public abstract class UpdateEtlCommand<T> : UpdateDatabaseCommand where T : EtlDestination
    {
        public readonly EtlConfiguration<T> Configuration;

        public readonly EtlType EtlType;

        protected UpdateEtlCommand() : base(null)
        {
            // for deserialization
        }

        protected UpdateEtlCommand(EtlConfiguration<T> configuration, EtlType type, string databaseName) : base(databaseName)
        {
            Configuration = configuration;
            EtlType = type;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
            json[nameof(EtlType)] = EtlType;
        }
    }

    public class UpdateRavenEtlCommand : UpdateEtlCommand<RavenDestination>
    {
        public UpdateRavenEtlCommand()
        {
            // for deserialization
        }

        public UpdateRavenEtlCommand(EtlConfiguration<RavenDestination> configuration, string databaseName) : base(configuration, EtlType.Raven, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteEtlCommand(EtlConfigurationNameRetriever.GetName(Configuration.Destination), EtlType, DatabaseName).UpdateDatabaseRecord(record, etag);
            new AddRavenEtlCommand(Configuration, DatabaseName).UpdateDatabaseRecord(record, etag);

            return null;
        }
    }

    public class UpdateSqlEtlCommand : UpdateEtlCommand<SqlDestination>
    {
        public UpdateSqlEtlCommand()
        {
            // for deserialization
        }

        public UpdateSqlEtlCommand(EtlConfiguration<SqlDestination> configuration, string databaseName) : base(configuration, EtlType.Sql, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteEtlCommand(EtlConfigurationNameRetriever.GetName(Configuration.Destination), EtlType, DatabaseName).UpdateDatabaseRecord(record, etag);
            new AddSqlEtlCommand(Configuration, DatabaseName).UpdateDatabaseRecord(record, etag);

            return null;
        }
    }
}