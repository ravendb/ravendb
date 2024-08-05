using System;
using System.Data;
using System.Data.Common;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.ETL.Relational.Metrics;
using Raven.Server.Documents.ETL.Relational.RelationalWriters;
using Snowflake.Data.Client;
using Sparrow.Json;
using DbCommandBuilder = Raven.Server.Documents.ETL.Relational.RelationalWriters.DbCommandBuilder;

namespace Raven.Server.Documents.ETL.Providers.Snowflake.RelationalWriters;

public class SnowflakeDatabaseWriterSimulator: RelationalDatabaseWriterSimulatorBase<SnowflakeConnectionString,SnowflakeEtlConfiguration>
{
    private readonly SnowflakeEtlConfiguration _snowflakeEtlConfiguration;
    public SnowflakeDatabaseWriterSimulator(SnowflakeEtlConfiguration configuration, DocumentDatabase database, RelationalDatabaseEtlMetricsCountersManager metricsCountersManager, EtlProcessStatistics statistics) : base(configuration, database, metricsCountersManager, statistics)
    {
        _snowflakeEtlConfiguration = configuration;
    }

    protected override DbCommandBuilder GetInitializedCommandBuilder()
    {
        return new DbCommandBuilder { Start =  string.Empty, End = string.Empty };
    }

    protected override DbProviderFactory GetDbProviderFactory(EtlConfiguration<SnowflakeConnectionString> configuration)
    {
        return SnowflakeDbFactory.Instance;
    }

    protected override void CreateAlertCannotOpenConnection(DocumentDatabase database, string etlConfigurationName, string connectionStringName, Exception e)
    {
        // empty by design
    }

    public override bool ParametrizeDeletes => false;
    
    protected override string GetConnectionString(EtlConfiguration<SnowflakeConnectionString> configuration)
    {
        return _snowflakeEtlConfiguration.Connection.ConnectionString;
    }

    protected override int? GetCommandTimeout()
    {
        return _snowflakeEtlConfiguration.CommandTimeout;
    }

    protected override bool ShouldQuoteTables()
    {
        return false;
    }

    protected override string GetParameterNameForDbParameter(string paramName)
    {
        return paramName;
    }

    protected override string GetParameterNameForCommandString(string targetParamName, bool parseJson)
    {
        return parseJson ? $"PARSE_JSON(:{targetParamName})" : $":{targetParamName}";
    }

    protected override void EnsureParamTypeSupportedByDbProvider(DbParameter parameter)
    {
        if (parameter.DbType == DbType.AnsiString)
            parameter.DbType = DbType.String;
    }

    protected override void SetPrimaryKeyParamValue(ToRelationalDatabaseItem itemToReplicate, DbParameter pkParam)
    {

        pkParam.Value = itemToReplicate.DocumentId.ToString();
        EnsureParamTypeSupportedByDbProvider(pkParam);
    }

    protected override string GetPostInsertIntoStartSyntax(ToRelationalDatabaseItem itemToReplicate)
    {
        return "SELECT ";
    }

    protected override string GetPostInsertIntoEndSyntax(ToRelationalDatabaseItem itemToReplicate)
    {
        return string.Empty;
    }

    protected override string GetPostDeleteSyntax(ToRelationalDatabaseItem itemToDelete)
    {
        return string.Empty;
    }

    protected override void HandleCustomDbTypeObject(DbParameter colParam, RelationalDatabaseColumn column, object dbType, object fieldValue, BlittableJsonReaderObject objectValue)
    {
        column.IsArrayOrObject = true;
        var dbTypeString = dbType.ToString() ?? string.Empty;
        colParam.Value = dbTypeString switch
        {
            "Array" when fieldValue is BlittableJsonReaderArray bjrav => bjrav.ToString(),
            "Object" when fieldValue is BlittableJsonReaderObject bjro => bjro.ToString(),
            _ => throw new NotSupportedException($"Type {dbTypeString} isn't currently supported by Snowflake ETL.")
        };
    }
}
