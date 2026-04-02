using System.Collections.Generic;
using System.Data.Common;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Server.Documents.CdcSink;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.CdcSink
{
    public class AddCdcSinkCommand : UpdateDatabaseRecordFeaturesCommand
    {
        public CdcSinkConfiguration Configuration { get; protected set; }

        public AddCdcSinkCommand()
        {
            // for deserialization
        }

        public AddCdcSinkCommand(CdcSinkConfiguration configuration, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(ref record.CdcSinks, record, etag);
        }

        private void Add(ref List<CdcSinkConfiguration> cdcSinks, DatabaseRecord record, long etag)
        {
            if (string.IsNullOrEmpty(Configuration.Name))
                Configuration.Name = record.EnsureUniqueTaskName(Configuration.GetDefaultTaskName());

            EnsureTaskNameIsNotUsed(record, Configuration.Name);

            AutoFillPostgresSettings(record);

            Configuration.TaskId = etag;

            cdcSinks ??= [];
            cdcSinks.Add(Configuration);
        }

        /// <summary>
        /// Auto-fills PublicationName and SlotName with deterministic hash-based names
        /// when the user didn't provide them. Only applies to PostgreSQL connections.
        /// </summary>
        private void AutoFillPostgresSettings(DatabaseRecord record)
        {
            if (Configuration.ConnectionStringName == null)
                return;

            if (record.SqlConnectionStrings.TryGetValue(Configuration.ConnectionStringName, out var connectionString) == false)
                return;

            if (connectionString.FactoryName != "Npgsql")
                return;

            Configuration.Postgres ??= new CdcSinkPostgresSettings();

            if (Configuration.Postgres.PublicationName == null || Configuration.Postgres.SlotName == null)
            {
                var tableNames = Configuration.CollectAllSourceTableNames("public");
                var dbName = ExtractDatabaseName(connectionString);

                Configuration.Postgres.PublicationName ??=
                    CdcSinkSourceVerifier.ComputePublicationName(dbName, Configuration.Name, tableNames);
                Configuration.Postgres.SlotName ??=
                    CdcSinkSourceVerifier.ComputeSlotName(dbName, Configuration.Name, tableNames);
            }
        }

        private static string ExtractDatabaseName(SqlConnectionString connectionString)
        {
            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString.ConnectionString };
            if (builder.TryGetValue("Database", out var db))
                return db.ToString();
            if (builder.TryGetValue("Initial Catalog", out db))
                return db.ToString();
            return "unknown";
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override bool Disabled => Configuration.Disabled;
    }
}
