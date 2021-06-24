using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Elasticsearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public abstract class AddEtlCommand<T, TConnectionString> : UpdateDatabaseCommand where T : EtlConfiguration<TConnectionString> where TConnectionString : ConnectionString
    {
        public T Configuration { get; protected set; }

        protected AddEtlCommand()
        {
            // for deserialization
        }

        protected AddEtlCommand(T configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        protected void Add(ref List<T> etls, DatabaseRecord record, long etag)
        {
            if (string.IsNullOrEmpty(Configuration.Name))
            {
                Configuration.Name = record.EnsureUniqueTaskName(Configuration.GetDefaultTaskName());
            }

            EnsureTaskNameIsNotUsed(record, Configuration.Name);

            Configuration.TaskId = etag;

            if (etls == null)
                etls = new List<T>();

            etls.Add(Configuration);
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

        public AddRavenEtlCommand(RavenEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(ref record.RavenEtls, record, etag);
        }
    }

    public class AddSqlEtlCommand : AddEtlCommand<SqlEtlConfiguration, SqlConnectionString>
    {
        public AddSqlEtlCommand()
        {
            // for deserialization
        }

        public AddSqlEtlCommand(SqlEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(ref record.SqlEtls, record, etag);
        }
    }

    public class AddElasticsearchEtlCommand : AddEtlCommand<ElasticsearchEtlConfiguration, ElasticsearchConnectionString>
    {
        public AddElasticsearchEtlCommand()
        {
            // for deserialization
        }

        public AddElasticsearchEtlCommand(ElasticsearchEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(ref record.ElasticsearchEtls, record, etag);
        }
    }

    public class AddOlapEtlCommand : AddEtlCommand<OlapEtlConfiguration, OlapConnectionString>
    {
        public AddOlapEtlCommand()
        {
            // for deserialization
        }

        public AddOlapEtlCommand(OlapEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(ref record.OlapEtls, record, etag);
        }
    }
}
