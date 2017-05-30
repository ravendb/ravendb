using System;
using System.Collections.Generic;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public class AddEtlCommand<T> : UpdateDatabaseCommand where T : EtlDestination
    {
        public readonly EtlConfiguration<T> Configuration;
        public readonly EtlType EtlType;

        public AddEtlCommand() : base(null)
        {
            // for deserialization
        }

        public AddEtlCommand(EtlConfiguration<T> configuration, EtlType etlType, string databaseName) : base(databaseName)
        {
            Configuration = configuration;
            EtlType = etlType;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            switch (EtlType)
            {
                case EtlType.Raven:
                    Add(record.RavenEtls, Configuration as EtlConfiguration<RavenDestination>);
                    return null;
                case EtlType.Sql:
                    Add(record.SqlEtls, Configuration as EtlConfiguration<SqlDestination>);
                    return null;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration destination type: {EtlType}");
            }
        }

        private void Add<TDest>(List<EtlConfiguration<TDest>> etls, EtlConfiguration<TDest> configuration) where TDest : EtlDestination
        {
            if (etls == null)
                etls = new List<EtlConfiguration<TDest>>();

            etls.Add(configuration);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
            json[nameof(EtlType)] = EtlType;
        }
    }
}