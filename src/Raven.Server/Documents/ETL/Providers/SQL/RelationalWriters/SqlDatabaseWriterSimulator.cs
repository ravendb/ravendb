using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.ETL.Relational.Metrics;
using Raven.Server.Documents.ETL.Relational.RelationalWriters;
using Sparrow.Json;
using DbCommandBuilder = Raven.Server.Documents.ETL.Relational.RelationalWriters.DbCommandBuilder;

namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;

public class SqlDatabaseWriterSimulator: RelationalDatabaseWriterSimulatorBase<SqlConnectionString, SqlEtlConfiguration>
{
    private readonly bool _isSqlServerFactoryType;
    private readonly SqlEtlConfiguration _sqlEtlConfiguration;
    private readonly string _sqlEtlTag = "SQL ETL";
    private readonly SqlProvider _providerType;

    public SqlDatabaseWriterSimulator(SqlEtlConfiguration configuration, DocumentDatabase database, RelationalDatabaseEtlMetricsCountersManager etlMetricsCountersManager,
        EtlProcessStatistics etlProcessStatistics) : base(configuration, database, etlMetricsCountersManager, etlProcessStatistics)
    {
        _sqlEtlConfiguration = configuration;
        ParametrizeDeletes = configuration.ParameterizeDeletes;
        _isSqlServerFactoryType = SqlDatabaseWriter.SqlServerFactoryNames.Contains(_sqlEtlConfiguration.Connection.FactoryName);
        _providerType = SqlProviderParser.GetSupportedProvider(_sqlEtlConfiguration.Connection.FactoryName);
    }
    
    protected override DbProviderFactory GetDbProviderFactory(EtlConfiguration<SqlConnectionString> configuration)
    {
        return DbProviderFactories.GetFactory(configuration.Connection.FactoryName);
    }

    protected override void CreateAlertCannotOpenConnection(DocumentDatabase database, string etlConfigurationName, string connectionStringName, Exception e)
    {
        // empty by design
    }

    protected override int? GetCommandTimeout()
    {
        return _sqlEtlConfiguration.CommandTimeout;
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

    protected override bool ShouldQuoteTables()
    {
        return _sqlEtlConfiguration.QuoteTables;
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

    protected override void SetPrimaryKeyParamValue(ToRelationalDatabaseItem itemToReplicate, DbParameter pkParam)
    {
        pkParam.Value = itemToReplicate.DocumentId.ToString();
    }

    protected override string GetPostInsertIntoStartSyntax(ToRelationalDatabaseItem itemToReplicate)
    {
        return "\r\nVALUES(";
    }

    protected override string GetPostInsertIntoEndSyntax(ToRelationalDatabaseItem itemToReplicate)
    {
        return _isSqlServerFactoryType && _sqlEtlConfiguration.ForceQueryRecompile ? ") OPTION(RECOMPILE)" : ")";
    }

    protected override string GetPostDeleteSyntax(ToRelationalDatabaseItem itemToDelete)
    {
        return _isSqlServerFactoryType && _sqlEtlConfiguration.ForceQueryRecompile ? " OPTION(RECOMPILE)" : string.Empty;
    }

    protected override void HandleCustomDbTypeObject(DbParameter colParam, RelationalDatabaseColumn column, object dbType, object fieldValue, BlittableJsonReaderObject objectValue)
    {
        var dbTypeString = dbType.ToString() ?? string.Empty;

        bool useGenericDbType = Enum.TryParse(dbTypeString, ignoreCase: false, out DbType type);

        if (useGenericDbType)
        {
            var value = fieldValue.ToString();

            try
            {
                colParam.DbType = type;
            }
            catch
            {
                if (type == DbType.Guid && Guid.TryParse(value, out var guid1) && colParam is OracleParameter oracleParameter)
                {
                    var arr = guid1.ToByteArray();
                    oracleParameter.Value = arr;
                    oracleParameter.OracleDbType = OracleDbType.Raw;
                    oracleParameter.Size = arr.Length;
                    return;
                }

                throw;
            }

            if (colParam.DbType == DbType.Guid && Guid.TryParse(value, out var guid))
            {
                if (colParam is Npgsql.NpgsqlParameter || colParam is SqlParameter)
                    colParam.Value = guid;

                if (colParam is MySqlConnector.MySqlParameter mySqlConnectorParameter)
                {
                    var arr = guid.ToByteArray();
                    mySqlConnectorParameter.Value = arr;
                    mySqlConnectorParameter.MySqlDbType = MySqlConnector.MySqlDbType.Binary;
                    mySqlConnectorParameter.Size = arr.Length;
                    return;
                }
            }
            else
            {
                colParam.Value = value;
            }
        }
        else
        {
            SqlDatabaseWriter.SetProviderSpecificDbType(dbTypeString, ref colParam, _providerType);

            if (fieldValue is IEnumerable<object> enumerableValue)
            {
                Type detectedType = null;

                colParam.Value = enumerableValue.Select(x =>
                {
                    if (x is IConvertible)
                    {
                        detectedType ??= SqlDatabaseWriter.TryDetectCollectionType(dbTypeString, x);

                        if (detectedType != null)
                            return Convert.ChangeType(x, detectedType);

                        return x.ToString();
                    }

                    return x.ToString();
                }).ToArray();
            }
            else
            {
                colParam.Value = fieldValue.ToString();
            }
        }

        if (objectValue.TryGetMember(nameof(SqlDocumentTransformer.VarcharFunctionCall.Size), out object size))
        {
            colParam.Size = (int)(long)size;
        }
    }
}
