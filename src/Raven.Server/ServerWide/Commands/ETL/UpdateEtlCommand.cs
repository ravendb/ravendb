using System;
using System.Collections.Generic;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public class UpdateEtlCommand<T> : UpdateDatabaseCommand where T : EtlDestination
    {
        public readonly EtlConfiguration<T> Configuration;

        public readonly EtlType EtlType;

        private readonly string _databaseName;

        public UpdateEtlCommand() : base(null)
        {
            // for deserialization
        }

        public UpdateEtlCommand(EtlConfiguration<T> configuration, EtlType type, string databaseName) : base(databaseName)
        {
            Configuration = configuration;
            EtlType = type;
            _databaseName = databaseName;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteEtlCommand(EtlConfigurationNameRetriever.GetName(Configuration.Destination), EtlType, _databaseName).UpdateDatabaseRecord(record, etag);
            new AddEtlCommand<T>(Configuration, EtlType, _databaseName).UpdateDatabaseRecord(record, etag);

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
            json[nameof(EtlType)] = EtlType;
        }
    }
}