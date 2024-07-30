using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.Data.SqlClient;
using NpgsqlTypes;
using Oracle.ManagedDataAccess.Client;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.ETL.Relational.Metrics;
using Raven.Server.Documents.ETL.Relational.RelationalWriters;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using DbCommandBuilder = Raven.Server.Documents.ETL.Relational.RelationalWriters.DbCommandBuilder;
namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;

internal sealed class SqlDatabaseWriter: RelationalDatabaseWriterBase<SqlConnectionString, SqlEtlConfiguration>
{
    private readonly string SqlEtlTag = "SQL ETL";
    private readonly bool _isSqlServerFactoryType;

    public static readonly string[] SqlServerFactoryNames =
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
        // empty by design - sql etl should support all db types todo: try to make it sure in SetValue method
    }

    protected override void  SetPrimaryKeyParamValue(ToRelationalDatabaseItem itemToReplicate, DbParameter pkParam)
    { 
        pkParam.Value = itemToReplicate.DocumentId.ToString();
    }

    protected override string GetPostInsertIntoStartSyntax(ToRelationalDatabaseItem itemToReplicate)
    {
        return "\r\nVALUES(";
    }

    protected override string GetPostInsertIntoEndSyntax(ToRelationalDatabaseItem itemToReplicate)
    {
        return _isSqlServerFactoryType && Configuration.ForceQueryRecompile ? ") OPTION(RECOMPILE)" : ")";
    }

    protected override string GetPostDeleteSyntax(ToRelationalDatabaseItem itemToDelete)
    {
        return _isSqlServerFactoryType && Configuration.ForceQueryRecompile ? " OPTION(RECOMPILE)" : string.Empty;
    }
    
    internal static void SetProviderSpecificDbType(string dbTypeString, ref DbParameter colParam, SqlProvider? providerType)
    {
        if (providerType is null)
        {
            return;
        }
        switch (providerType)
        {
            case SqlProvider.SqlClient:
                SqlDbType sqlDbType = ParseProviderSpecificParameterType<SqlDbType>(dbTypeString);
                ((SqlParameter)colParam).SqlDbType = sqlDbType;
                break;
            case SqlProvider.Npgsql:
                NpgsqlDbType npgsqlType = ParseProviderSpecificParameterType<NpgsqlDbType>(dbTypeString);
                ((Npgsql.NpgsqlParameter)colParam).NpgsqlDbType = npgsqlType;
                break;
            case SqlProvider.MySqlClient:
            case SqlProvider.MySqlConnectorFactory:
                MySqlConnector.MySqlDbType mySqlConnectorDbType = ParseProviderSpecificParameterType<MySqlConnector.MySqlDbType>(dbTypeString);
                ((MySqlConnector.MySqlParameter)colParam).MySqlDbType = mySqlConnectorDbType;
                break;
            case SqlProvider.OracleClient:
                OracleDbType oracleDbType = ParseProviderSpecificParameterType<OracleDbType>(dbTypeString);
                ((OracleParameter)colParam).OracleDbType = oracleDbType;
                break;
            default:
                ThrowProviderNotSupported(providerType);
                break;
        }

    }
    
    private static T ParseProviderSpecificParameterType<T>(string dbTypeString) where T : struct, Enum, IConvertible
    {
        if (dbTypeString.Contains("|"))
        {
            var multipleTypes = dbTypeString.Split('|').Select(e =>
            {
                if (Enum.TryParse(e.Trim(), ignoreCase: true, out T singleProviderSpecificType) == false)
                    ThrowCouldNotParseDbType();

                return singleProviderSpecificType;
            }).ToList();

            return multipleTypes.Aggregate((a, b) => (T)Enum.ToObject(typeof(T), Convert.ToInt32(a) | Convert.ToInt32(b)));
        }

        if (Enum.TryParse(dbTypeString, ignoreCase: true, out T providerSpecificType) == false)
            ThrowCouldNotParseDbType();

        return providerSpecificType;

        void ThrowCouldNotParseDbType()
        {
            throw new InvalidOperationException(string.Format($"Couldn't parse '{dbTypeString}' as db type."));
        }
    }

    static void ThrowProviderNotSupported(SqlProvider? providerType)
    {
        throw new NotSupportedException($"Factory provider '{providerType}' is not supported");
    }
}
