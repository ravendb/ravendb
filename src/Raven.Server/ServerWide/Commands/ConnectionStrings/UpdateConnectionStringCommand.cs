using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ConnectionStrings
{
    public abstract class UpdateConnectionStringCommand<T> : UpdateDatabaseCommand where T : ConnectionString
    {
        public string OldConnectionStringName { get; protected set; }
        public T ConnectionString { get; protected set; }

        protected UpdateConnectionStringCommand() : base(null)
        {
            // for deserialization
        }

        protected UpdateConnectionStringCommand(string oldConnectionStringName, T connectionString,  string databaseName) : base(databaseName)
        {
            OldConnectionStringName = oldConnectionStringName;
            ConnectionString = connectionString;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(OldConnectionStringName)] = OldConnectionStringName;
            json[nameof(ConnectionString)] = TypeConverter.ToBlittableSupportedType(ConnectionString);
        }
    }

    public class UpdateRavenConnectionString : UpdateConnectionStringCommand<RavenConnectionString>
    {
        protected UpdateRavenConnectionString()
        {
            // for deserialization
        }

        public UpdateRavenConnectionString(string oldConnectionStringName, RavenConnectionString connectionString, string databaseName) : base(oldConnectionStringName, connectionString, databaseName)
        {            
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.RavenConnectionStrings.Remove(OldConnectionStringName);
            record.RavenConnectionStrings[ConnectionString.Name] = ConnectionString;
            return null;
        }
    }

    public class UpdateSqlConnectionString : UpdateConnectionStringCommand<SqlConnectionString>
    {
        protected UpdateSqlConnectionString()
        {
            // for deserialization
        }

        public UpdateSqlConnectionString(string oldConnectionStringName, SqlConnectionString connectionString, string databaseName) : base(oldConnectionStringName, connectionString, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.SqlConnectionStrings.Remove(OldConnectionStringName);
            record.SqlConnectionStrings[ConnectionString.Name] = ConnectionString;
            return null;
        }
    }
}