using System;

namespace Raven.Client.Documents.Operations.ETL.SQL
{
    public class SqlProviderParser
    {
        public static SqlProvider GetSupportedProvider(string factoryName)
        {
            switch (factoryName)
            {
                case "Microsoft.Data.SqlClient":
                    return SqlProvider.SqlClient;
                case "Npgsql":
                    return SqlProvider.Npgsql;
                case "System.Data.SqlServerCe.4.0":
                    throw new NotImplementedException($"Factory '{factoryName}' is not implemented yet");
                    //return SqlProvider.SqlServerCe_4_0;
                case "System.Data.OleDb":
                    throw new NotImplementedException($"Factory '{factoryName}' is not implemented yet");
                    //return SqlProvider.OleDb;
                case "Oracle.ManagedDataAccess.Client":
                    return SqlProvider.OracleClient;
                case "MySql.Data.MySqlClient":
                case "MySqlConnector.MySqlConnectorFactory":
                    return SqlProvider.MySqlConnectorFactory;
                case "System.Data.SqlServerCe.3.5":
                    throw new NotImplementedException($"Factory '{factoryName}' is not implemented yet");
                    //return SqlProvider.SqlServerCe_3_5;
                default:
                    throw new NotSupportedException($"Factory '{factoryName}' is not supported");
            }
        }
    }
}
