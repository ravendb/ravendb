﻿using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.Snowflake;
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

    public sealed class AddRavenEtlCommand : AddEtlCommand<RavenEtlConfiguration, RavenConnectionString>
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

    public sealed class AddSqlEtlCommand : AddEtlCommand<SqlEtlConfiguration, SqlConnectionString>
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

    public sealed class AddElasticSearchEtlCommand : AddEtlCommand<ElasticSearchEtlConfiguration, ElasticSearchConnectionString>
    {
        public AddElasticSearchEtlCommand()
        {
            // for deserialization
        }

        public AddElasticSearchEtlCommand(ElasticSearchEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(ref record.ElasticSearchEtls, record, etag);
        }
    }

    public sealed class AddOlapEtlCommand : AddEtlCommand<OlapEtlConfiguration, OlapConnectionString>
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

    public sealed class AddQueueEtlCommand : AddEtlCommand<QueueEtlConfiguration, QueueConnectionString>
    {
        public AddQueueEtlCommand()
        {
            // for deserialization
        }

        public AddQueueEtlCommand(QueueEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(ref record.QueueEtls, record, etag);
        }
    }
    
    public sealed class AddSnowflakeEtlCommand : AddEtlCommand<SnowflakeEtlConfiguration, SnowflakeConnectionString>
    {
        public AddSnowflakeEtlCommand()
        {
            // for deserialization
        }

        public AddSnowflakeEtlCommand(SnowflakeEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(ref record.SnowflakeEtls, record, etag);
        }
    }
}
