using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
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

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201, serverStore) == false)
                return;

            var licenseStatus = serverStore.Cluster.GetLicenseStatus(context);

            if (licenseStatus.HasRavenEtl)
                return;

            if (databaseRecord.RavenEtls.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.RavenEtl, "Your license doesn't support adding Raven ETL feature.");

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

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201, serverStore) == false)
                return;

            var licenseStatus = serverStore.Cluster.GetLicenseStatus(context);

            if (licenseStatus.HasSqlEtl)
                return;

            if (databaseRecord.SqlEtls.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.SqlEtl, "Your license doesn't support adding SQL ETL feature.");

        }
    }

    public class AddElasticSearchEtlCommand : AddEtlCommand<ElasticSearchEtlConfiguration, ElasticSearchConnectionString>
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

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201, serverStore) == false)
                return;

            var licenseStatus = serverStore.Cluster.GetLicenseStatus(context);

            if (licenseStatus.HasElasticSearchEtl)
                return;

            if (databaseRecord.ElasticSearchEtls.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.ElasticSearchEtl, "Your license doesn't support adding Elasticsearch ETL feature.");

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

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201, serverStore) == false)
                return;

            var licenseStatus = serverStore.Cluster.GetLicenseStatus(context);

            if (licenseStatus.HasOlapEtl)
                return;

            if (databaseRecord.OlapEtls.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.OlapEtl, "Your license doesn't support adding Olap ETL feature.");
        }
    }

    public class AddQueueEtlCommand : AddEtlCommand<QueueEtlConfiguration, QueueConnectionString>
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

        public override void AssertLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201, serverStore) == false)
                return;

            var licenseStatus = serverStore.Cluster.GetLicenseStatus(context);

            if (licenseStatus.HasQueueEtl)
                return;

            if (databaseRecord.QueueEtls.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.QueueEtl, "Your license doesn't support adding Queue ETL feature.");

        }
    }
}
