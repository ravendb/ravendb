using System;
using System.Data.Common;
using System.Linq;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.ETL.Relational.Metrics;
using Raven.Server.Documents.ETL.Relational.RelationalWriters;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using DbCommandBuilder = Raven.Server.Documents.ETL.Relational.RelationalWriters.DbCommandBuilder;
namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;

internal sealed class SqlDatabaseWriter: RelationalWriterBase<SqlConnectionString, SqlEtlConfiguration>
{
    private readonly string SqlEtlTag = "SQL ETL";
    private readonly bool _isSqlServerFactoryType;

    private static readonly string[] SqlServerFactoryNames =
    {
        "System.Data.SqlClient", "Microsoft.Data.SqlClient", "System.Data.SqlServerCe.4.0", "MySql.Data.MySqlClient", "System.Data.SqlServerCe.3.5"
    }; 
    
    private readonly SqlProvider _providerType;

    private new readonly SqlEtlConfiguration Configuration; // todo: get rid of bs with this 'new' hiding the same thing beneath 

    public SqlDatabaseWriter(DocumentDatabase database, SqlEtlConfiguration configuration,
        RelationalEtlMetricsCountersManager metrics, EtlProcessStatistics statistics) : base(database,
        configuration, metrics, statistics)
    {
        Configuration = configuration;
        ParametrizeDeletes = configuration.ParameterizeDeletes;
        if (SqlServerFactoryNames.Contains(configuration.Connection.FactoryName))
        {
            _isSqlServerFactoryType = true;
        }

        _providerType = SqlProviderParser.GetSupportedProvider(Configuration.Connection.FactoryName);
    }

    public override bool ParametrizeDeletes { get; }

    protected override string GetConnectionString(EtlConfiguration<SqlConnectionString> configuration)
    {
        return configuration.Connection.ConnectionString;
    }

    protected override DbCommandBuilder GetInitializedCommandBuilder()
    {
        return ProviderFactory.InitializeCommandBuilder();
    }

    protected override DbProviderFactory GetDbProviderFactory(EtlConfiguration<SqlConnectionString> configuration)
    {
        DbProviderFactory providerFactory;
        
        try
        {
            providerFactory = DbProviderFactories.GetFactory(configuration.Connection.FactoryName);
        }
        catch (Exception e)
        {
            var message = $"Could not find provider factory {configuration.Connection.FactoryName} to replicate to sql for {configuration.Name}, ignoring.";

            if (Logger.IsInfoEnabled)
                Logger.Info(message, e);

            Database.NotificationCenter.Add(AlertRaised.Create(
                Database.Name,
                SqlEtlTag,
                message,
                AlertType.SqlEtl_ProviderError,
                NotificationSeverity.Error,
                details: new ExceptionDetails(e)));

            throw;
        }
        return providerFactory;
    }

    protected override void CreateAlertCannotOpenConnection(DocumentDatabase database, string etlConfigurationName, string connectionStringName, Exception e)
    {
        database.NotificationCenter.Add(AlertRaised.Create(
            database.Name,
            SqlEtlTag,
            $"[{etlConfigurationName}] Could not open connection using '{connectionStringName}' connection string",
            AlertType.SqlEtl_ConnectionError,
            NotificationSeverity.Error,
            key: $"{etlConfigurationName}/{connectionStringName}",
            details: new ExceptionDetails(e)));
    }

    protected override int? GetCommandTimeout()
    {
        return Configuration.CommandTimeout;
    }

    protected override bool ShouldQuoteTables()
    {
        return Configuration.QuoteTables;
    }

    protected override string GetParameterNameForDbParameter(string paramName)
    {
        return GetParameterNameForCommandString(paramName, false);
    }

    protected override string GetParameterNameForCommandString(string targetParamName, bool parseJson)
    {
        switch (ProviderFactory.GetType().Name)
        {
            case "SqlClientFactory":
            case "MySqlClientFactory":
            case "MySqlConnectorFactory":
                return "@" + targetParamName;

            case "OracleClientFactory":
            case "NpgsqlFactory":
                return ":" + targetParamName;

            default:
                throw new NotSupportedException($"Unhandled provider factory: {ProviderFactory.GetType().Name}");
        }
    }

    protected override void EnsureParamTypeSupportedByDbProvider(DbParameter parameter)
    {
        // empty
    }

    protected override void  SetPrimaryKeyParamValue(ToRelationalItem itemToReplicate, DbParameter pkParam)
    { 
        pkParam.Value = itemToReplicate.DocumentId.ToString();
    }

    protected override string GetPostInsertIntoStartSyntax(ToRelationalItem itemToReplicate)
    {
        return "\r\nVALUES(";
    }

    protected override string GetPostInsertIntoEndSyntax(ToRelationalItem itemToReplicate)
    {
        return _isSqlServerFactoryType && Configuration.ForceQueryRecompile ? ") OPTION(RECOMPILE)" : ")";
    }

    protected override string GetPostDeleteSyntax(ToRelationalItem itemToDelete)
    {
        return _isSqlServerFactoryType && Configuration.ForceQueryRecompile ? " OPTION(RECOMPILE)" : string.Empty;
    }
}
