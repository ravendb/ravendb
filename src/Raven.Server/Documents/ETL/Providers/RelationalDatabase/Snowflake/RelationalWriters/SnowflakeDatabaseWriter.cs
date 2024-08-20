using System;
using System.Data;
using System.Data.Common;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Metrics;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.RelationalWriters;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Snowflake.Data.Client;
using Sparrow.Json;
using DbCommandBuilder = Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.RelationalWriters.DbCommandBuilder;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Snowflake.RelationalWriters;

public class SnowflakeDatabaseWriter: RelationalDatabaseWriterBase<SnowflakeConnectionString, SnowflakeEtlConfiguration>
{
    private const string SnowflakeEtlTag = "Snowflake ETL";

    private readonly SnowflakeEtlConfiguration _snowflakeEtlConfiguration;

    public SnowflakeDatabaseWriter(DocumentDatabase database, SnowflakeEtlConfiguration configuration, RelationalDatabaseEtlMetricsCountersManager sqlMetrics,
        EtlProcessStatistics statistics, bool shouldConnectToTarget = true) : base(database, configuration, sqlMetrics, statistics, shouldConnectToTarget)
    {
        _snowflakeEtlConfiguration = configuration;
    }

    public override bool ParametrizeDeletes => false;

    protected override string GetConnectionString(EtlConfiguration<SnowflakeConnectionString> configuration)
    {
        return configuration.Connection.ConnectionString;
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
        database.NotificationCenter.Add(AlertRaised.Create(
            database.Name,
            SnowflakeEtlTag,
            $"[{etlConfigurationName}] Could not open connection using '{connectionStringName}' connection string",
            AlertType.SnowflakeEtl_ConnectionError,
            NotificationSeverity.Error,
            key: $"{etlConfigurationName}/{connectionStringName}",
            details: new ExceptionDetails(e)));
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
        // By default, DbParameter has AnsiString DbType. SetParamValue changes it, so we don't care
        // At null value, we can't update DbType, because there's no DbType.Null. DbType stays AnsiString
        // Snowflake doesn't support AnsiString - we need to correct that to DbType.String
        // Only Snowflake has this, so we need granular code, to not break Sql
        if (parameter.DbType == DbType.AnsiString)
            parameter.DbType = DbType.String;
    }

    protected override void SetPrimaryKeyParamValue(RelationalDatabaseItem itemToReplicate, DbParameter pkParam)
    {
        pkParam.Value = itemToReplicate.DocumentId.ToString();
        EnsureParamTypeSupportedByDbProvider(pkParam);
    }

    protected override string GetPostInsertIntoStartSyntax(RelationalDatabaseItem itemToReplicate)
    {
        return "SELECT ";
    }

    protected override string GetPostInsertIntoEndSyntax(RelationalDatabaseItem itemToReplicate)
    {
        return string.Empty;
    }

    protected override string GetPostDeleteSyntax(RelationalDatabaseItem itemToDelete)
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

    protected override string GetAfterDeleteWhereIdentifierBeforeInExtraSyntax()
    {
        return " COLLATE 'lower'";
    }

    public static void TestConnection(string connectionString)
    {
        var providerFactory = SnowflakeDbFactory.Instance;
        var connection = providerFactory.CreateConnection();
        connection.ConnectionString = connectionString;

        try
        {
            connection.Open();
            connection.Close();
        }
        finally
        {
            connection.Dispose();
        }
    }
}
