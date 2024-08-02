using System;
using System.Collections.Generic;
using System.Data.Common;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.ETL.Relational.RelationalWriters;
using Snowflake.Data.Client;
using DbCommandBuilder = Raven.Server.Documents.ETL.Relational.RelationalWriters.DbCommandBuilder;

namespace Raven.Server.Documents.ETL.Providers.Snowflake.RelationalWriters;

public class SnowflakeDatabaseWriterSimulator: RelationalDatabaseWriterSimulatorBase<SnowflakeEtlConfiguration, SnowflakeConnectionString>
{
    public SnowflakeDatabaseWriterSimulator(SnowflakeEtlConfiguration configuration) : base(configuration, configuration.ParameterizeDeletes)
    {
    }

    protected override DbProviderFactory GetDbProviderFactory(EtlConfiguration<SnowflakeConnectionString> configuration)
    {
        return new SnowflakeDbFactory();
    }

    protected override DbCommandBuilder GetInitializedCommandBuilder()
    {
        return new DbCommandBuilder {Start = "\"", End = "\""};
    }

    protected override DbParameter GetNewDbParameter()
    {
        return new SnowflakeDbParameter();
    }

    protected override string GetPostInsertIntoStartSyntax()
    {
        return "SELECT";
    }

    protected override string GetPostInsertIntoEndSyntax()
    {
        return string.Empty;
    }

    protected override string GetPostDeleteSyntax()
    {
        return string.Empty;
    }

    protected override void SetParamValue(DbParameter colParam, RelationalDatabaseColumn column, List<Func<DbParameter, string, bool>> stringParsers)
    {
        SnowflakeDatabaseWriter.SetParamValue(colParam, column, stringParsers, true);
    }

    protected override bool ShouldQuoteTables()
    {
        return false;
    }
}
