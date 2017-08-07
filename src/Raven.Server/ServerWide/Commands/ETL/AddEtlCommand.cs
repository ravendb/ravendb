using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public abstract class AddEtlCommand<T, TConnectionString> : UpdateDatabaseCommand where T : EtlConfiguration<TConnectionString> where TConnectionString : ConnectionString
    {
        public T Configuration { get; protected set; }

        protected AddEtlCommand() : base(null)
        {
            // for deserialization
        }

        protected AddEtlCommand(T configuration, string databaseName) : base(databaseName)
        {
            Configuration = configuration;
        }

        protected void Add(ref List<T> etls, T configuration)
        {
            if (etls == null)
                etls = new List<T>();

            etls.Add(configuration);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }

    public class AddRavenEtlCommand : AddEtlCommand<RavenEtlConfiguration, RavenConnectionString>
    {
        public AddRavenEtlCommand()
        {
            // for deserialization
        }

        public AddRavenEtlCommand(RavenEtlConfiguration configuration, string databaseName) : base(configuration, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Configuration.TaskId = etag;
            Add(ref record.RavenEtls, Configuration);
            return null;
        }
    }

    public class AddSqlEtlCommand : AddEtlCommand<SqlEtlConfiguration, SqlConnectionString>
    {
        public AddSqlEtlCommand()
        {
            // for deserialization
        }

        public AddSqlEtlCommand(SqlEtlConfiguration configuration, string databaseName) : base(configuration, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Configuration.TaskId = etag;
            Add(ref record.SqlEtls, Configuration);
            return null;
        }
    }
}