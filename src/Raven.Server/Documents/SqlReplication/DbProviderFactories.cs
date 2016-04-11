using System;
using System.Data.Common;
using System.Data.SqlClient;

namespace Raven.Server.Documents.SqlReplication
{
    public class DbProviderFactories
    {
        public static DbProviderFactory GetFactory(string factoryName)
        {
            switch (factoryName)
            {
                case "System.Data.SqlClient":
                    return SqlClientFactory.Instance;
                case "System.Data.SqlServerCe.4.0":
                case "System.Data.OleDb":
                case "System.Data.OracleClient":
                case "MySql.Data.MySqlClient":
                case "System.Data.SqlServerCe.3.5":
                case "Npgsql":
                    throw new NotImplementedException($"Factory '{factoryName}' is not implemented yet");
                default:
                    throw new NotSupportedException($"Factory '{factoryName}' is not supported");
            }
        }
    }

    public static class DbProviderFactoryExtensions
    {
        public static DbCommandBuilder CreateCommandBuilder(this DbProviderFactory factory)
        {
            return new DbCommandBuilder();
        }
    }
}