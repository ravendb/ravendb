using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Raven.Client.Documents.Operations.ETL.SQL;
using DbCommandBuilder = Raven.Server.Documents.ETL.Providers.RelationalDatabase.RelationalWriters.DbCommandBuilder;

namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters
{
    public sealed class DbProviderFactories
    {
        public static DbProviderFactory GetFactory(string factoryName)
        {
            switch (SqlProviderParser.GetSupportedProvider(factoryName))
            {
                case SqlProvider.SqlClient:
                    return SqlClientFactory.Instance;
                case SqlProvider.Npgsql:
                    return NpgsqlFactory.Instance;
                case SqlProvider.MySqlClient:
                case SqlProvider.MySqlConnectorFactory:
                    return MySqlConnector.MySqlConnectorFactory.Instance;
                case SqlProvider.OracleClient:
                    return OracleClientFactory.Instance;
                default:
                    throw new NotSupportedException($"Factory '{factoryName}' is not supported");
            }
        }
    }

    public static class DbProviderFactoryExtensions
    {
        public static DbCommandBuilder InitializeCommandBuilder(this DbProviderFactory factory)
        {
            if (factory is SqlClientFactory)
                return new DbCommandBuilder
                {
                    Start = "[",
                    End = "]"
                };
            if (factory is NpgsqlFactory || factory is OracleClientFactory)
                return new DbCommandBuilder
                {
                    Start = "\"",
                    End = "\""
                };
            return new DbCommandBuilder();
        }
    }
}
