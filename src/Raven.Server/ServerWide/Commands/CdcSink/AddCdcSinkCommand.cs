using System;
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

            AutoFillPostgresSettings(record);

            Configuration.TaskId = etag;

            cdcSinks ??= [];
            cdcSinks.Add(Configuration);
        }

        /// <summary>
        /// Auto-fills PublicationName and SlotName when the user didn't provide them.
        /// Uses a shared GUID with the rvn_cdc_ prefix to generate readable, unique names
        /// that fit within PostgreSQL's 63-character identifier limit.
        /// Only applies to PostgreSQL connections.
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
                // Use the same GUID for both so they're clearly paired.
                // "rvn_cdc_p_" + 32 hex chars = 42 chars (well under the 63-char PG limit)
                var id = Guid.NewGuid().ToString("N"); // 32 hex chars, no dashes
                Configuration.Postgres.PublicationName ??= $"rvn_cdc_p_{id}";
                Configuration.Postgres.SlotName ??= $"rvn_cdc_s_{id}";
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override bool Disabled => Configuration.Disabled;
    }
}
