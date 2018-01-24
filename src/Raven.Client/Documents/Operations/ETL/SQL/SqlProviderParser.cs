using System;

namespace Raven.Client.Documents.Operations.ETL.SQL
{
    public class SqlProviderParser
    {
        public static SqlProvider GetSupportedProvider(string factoryName)
        {
            switch (factoryName)
            {
                case "System.Data.SqlClient":
                    return SqlProvider.SqlClient;
                case "Npgsql":
                    return SqlProvider.Npgsql;
                case "System.Data.SqlServerCe.4.0":
                    throw new NotImplementedException($"Factory '{factoryName}' is not implemented yet");
                    //return SqlProvider.SqlServerCe_4_0;
                case "System.Data.OleDb":
                    throw new NotImplementedException($"Factory '{factoryName}' is not implemented yet");
                    //return SqlProvider.OleDb;
                case "System.Data.OracleClient":
                    throw new NotImplementedException($"Factory '{factoryName}' is not implemented yet");
                    //return SqlProvider.OracleClient;
                case "MySql.Data.MySqlClient":
                    throw new NotImplementedException($"Factory '{factoryName}' is not implemented yet");
                    //return SqlProvider.MySqlClient;
                case "System.Data.SqlServerCe.3.5":
                    throw new NotImplementedException($"Factory '{factoryName}' is not implemented yet");
                    //return SqlProvider.SqlServerCe_3_5;
                default:
                    throw new NotSupportedException($"Factory '{factoryName}' is not supported");
            }
        }
    }
}
