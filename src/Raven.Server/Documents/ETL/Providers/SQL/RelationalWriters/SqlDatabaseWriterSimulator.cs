using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Linq;
using Oracle.ManagedDataAccess.Client;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.ETL.Relational.RelationalWriters;
using DbCommandBuilder = Raven.Server.Documents.ETL.Relational.RelationalWriters.DbCommandBuilder;

namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;

public class SqlDatabaseWriterSimulator: RelationalDatabaseWriterSimulatorBase<SqlEtlConfiguration, SqlConnectionString>
{
    private SqlProvider _providerType;
    private readonly bool _isSqlServerFactoryType;

    private static readonly string[] SqlServerFactoryNames = SqlDatabaseWriter.SqlServerFactoryNames;
    
    private readonly string _sqlEtlTag = "SQL ETL";

    public SqlDatabaseWriterSimulator(SqlEtlConfiguration configuration) : base(configuration, configuration.ParameterizeDeletes)
    {
        _providerType = SqlProviderParser.GetSupportedProvider(Configuration.Connection.FactoryName);
        _isSqlServerFactoryType = SqlServerFactoryNames.Contains(Configuration.Connection.FactoryName);
    }

    protected override DbProviderFactory GetDbProviderFactory(EtlConfiguration<SqlConnectionString> configuration)
    {
        return DbProviderFactories.GetFactory(configuration.Connection.FactoryName);
    }

    protected override DbCommandBuilder GetInitializedCommandBuilder()
    {
        return ProviderFactory.InitializeCommandBuilder();
    }

    protected override DbParameter GetNewDbParameter()
    {
        switch (_providerType)
        {
            case SqlProvider.SqlClient:
                return new SqlParameter();
            case SqlProvider.Npgsql:
                return new Npgsql.NpgsqlParameter();
            case SqlProvider.MySqlClient:
            case SqlProvider.MySqlConnectorFactory:
                return new MySqlConnector.MySqlParameter();
            case SqlProvider.OracleClient:
                return new OracleParameter();
            default:
                throw new NotSupportedException($"Factory provider '{_providerType}' is not supported");
        }
    }

    protected override string GetPostInsertIntoStartSyntax()
    {
        return "\r\nVALUES(";
    }

    protected override string GetPostInsertIntoEndSyntax()
    {
        if (_isSqlServerFactoryType && Configuration.ForceQueryRecompile)
        {
            return ") OPTION(RECOMPILE)";
        }

        return string.Empty;
    }

    protected override string GetPostDeleteSyntax()
    {
        if (_isSqlServerFactoryType && Configuration.ForceQueryRecompile)
        {
            return " OPTION(RECOMPILE)";
        }

        return string.Empty;
    }

    protected override void SetParamValue(DbParameter colParam, RelationalDatabaseColumn column, List<Func<DbParameter, string, bool>> stringParsers)
    {
        SqlDatabaseWriter.SetParamValue(colParam, column, stringParsers, false);
    }

    protected override bool ShouldQuoteTables()
    {
        return Configuration.QuoteTables;
    }
}
