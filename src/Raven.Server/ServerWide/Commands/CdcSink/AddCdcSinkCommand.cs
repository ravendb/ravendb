using System.Collections.Generic;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.ServerWide;
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

            Configuration.TaskId = etag;

            AutoFillPostgresSettings(record, etag);

            cdcSinks ??= [];
            cdcSinks.Add(Configuration);
        }

        /// <summary>
        /// Auto-fills PublicationName and SlotName when the user didn't provide them.
        /// Derives both from the raft <paramref name="etag"/> (the task id) so the names are
        /// identical on every cluster node: this runs inside the deterministic Raft apply path.
        /// The etag is unique per command, and "rvn_cdc_p_" + etag stays well
        /// within PostgreSQL's 63-character identifier limit. Only applies to PostgreSQL connections.
        /// </summary>
        private void AutoFillPostgresSettings(DatabaseRecord record, long etag)
        {
            if (Configuration.ConnectionStringName == null)
                return;

            if (record.SqlConnectionStrings.TryGetValue(Configuration.ConnectionStringName, out var connectionString) == false)
                return;

            if (connectionString.FactoryName != "Npgsql")
                return;

            Configuration.Postgres ??= new CdcSinkPostgresSettings();

            // Same etag for both so they're clearly paired.
            Configuration.Postgres.PublicationName ??= $"rvn_cdc_p_{etag}";
            Configuration.Postgres.SlotName ??= $"rvn_cdc_s_{etag}";
        }


        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override bool Disabled => Configuration.Disabled;
    }
}
