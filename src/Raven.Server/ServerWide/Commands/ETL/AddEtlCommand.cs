using System;
using System.Collections.Generic;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public abstract class AddEtlCommand<T> : UpdateDatabaseCommand where T : EtlDestination
    {
        public readonly EtlConfiguration<T> Configuration;

        protected AddEtlCommand() : base(null)
        {
            // for deserialization
        }

        protected AddEtlCommand(EtlConfiguration<T> configuration, string databaseName) : base(databaseName)
        {
            Configuration = configuration;
        }

        protected void Add(List<EtlConfiguration<T>> etls, EtlConfiguration<T> configuration)
        {
            if (etls == null)
                etls = new List<EtlConfiguration<T>>();

            etls.Add(configuration);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }

    public class AddRavenEtlCommand : AddEtlCommand<RavenDestination>
    {
        public AddRavenEtlCommand()
        {
            // for deserialization
        }

        public AddRavenEtlCommand(EtlConfiguration<RavenDestination> configuration, string databaseName) : base(configuration, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(record.RavenEtls, Configuration);
            return null;
        }
    }

    public class AddSqlEtlCommand : AddEtlCommand<SqlDestination>
    {
        public AddSqlEtlCommand()
        {
            // for deserialization
        }

        public AddSqlEtlCommand(EtlConfiguration<SqlDestination> configuration, string databaseName) : base(configuration, databaseName)
        {

        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Add(record.SqlEtls, Configuration);
            return null;
        }
    }
}