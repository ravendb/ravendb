using System;

namespace Raven.Client.Documents.Operations.ETL.SQL
{
    internal static class SqlConnectionStringParser
    {
        internal static (string Database, string Server) GetDatabaseAndServerFromConnectionString(string factoryName, string connectionString)
        {
            string database;
            string server;

            switch (SqlProviderParser.GetSupportedProvider(factoryName))
            {
                case SqlProvider.SqlClient:
                    database = GetConnectionStringValue(connectionString, new[] { "Initial Catalog", "Database" });

                    if (database == null)
                        database = "master";

                    server = GetConnectionStringValue(connectionString, new[] { "Data Source", "Server", "Address", "Addr", "Network Address" });
                    break;
                case SqlProvider.Npgsql:
                    database = GetConnectionStringValue(connectionString, new[] { "Database" });
                    server = GetConnectionStringValue(connectionString, new[] { "Host", "Data Source", "Server" });

                    var postgrePort = GetConnectionStringValue(connectionString, new[] { "Port" });

                    if (string.IsNullOrEmpty(postgrePort))
                        server += $":{postgrePort}";
                    break;
                case SqlProvider.MySqlClient:
                    database = GetConnectionStringValue(connectionString, new[] { "Database", "Initial Catalog" });

                    if (database == null)
                        database = "mysql";

                    server = GetConnectionStringValue(connectionString, new[] { "Host", "Server", "Data Source", "DataSource", "Address", "Addr", "Network Address" });

                    if (server == null)
                        server = "localhost";

                    var mysqlPort = GetConnectionStringValue(connectionString, new[] { "Port" });

                    if (string.IsNullOrEmpty(mysqlPort) == false)
                        server += $":{mysqlPort}";
                    break;
                case SqlProvider.OracleClient:

                    server = null;
                    database = null;

                    var dataSource = GetConnectionStringValue(connectionString, new[] { "Data Source" });

                    if (string.IsNullOrEmpty(dataSource) == false)
                    {
                        server = GetOracleDataSourceSubValue(dataSource, "HOST");

                        if (server != null)
                        {
                            var port = GetOracleDataSourceSubValue(dataSource, "PORT");

                            if (port != null)
                                server += $":{port}";
                        }

                        database = GetOracleDataSourceSubValue(dataSource, "SERVICE_NAME") ?? GetOracleDataSourceSubValue(dataSource, "SID");

                        if (server == null)
                        {
                            var parts = dataSource.Split(new []{'@'}, 2);

                            if (parts.Length == 2)
                            {
                                // Data Source=username/password@myserver//instancename;
                                // Data Source=username/password@myserver/myservice:dedicated/instancename;

                                server = parts[1];
                            }
                            else
                            {
                                // Data Source=myOracleDB;
                                server = dataSource;
                            }
                        }
                    }

                    break;

                    
                default:
                    throw new NotSupportedException($"Factory '{factoryName}' is not supported");
            }

            return (database, server);
        }

        public static string GetConnectionStringValue(string connectionString, string[] keyNames)
        {
            var parts = connectionString.Split(';');

            foreach (var part in parts)
            {
                var keyValue = part.Split(new[] { '=' }, 2);

                foreach (var key in keyNames)
                {
                    if (keyValue[0].Trim().Equals(key, StringComparison.OrdinalIgnoreCase))
                        return keyValue[1].Trim();
                }
            }

            return null;
        }

        internal static string GetOracleDataSourceSubValue(string dataSourceValue, string key)
        {
            var indexOf = dataSourceValue.IndexOf(key, StringComparison.OrdinalIgnoreCase);

            if (indexOf == -1)
                return null;

            var subString = dataSourceValue.Substring(indexOf);

            var closingBracket = subString.IndexOf(')');

            if (closingBracket == -1)
                return null;

            var subValue = subString.Substring(0, closingBracket);

            return GetConnectionStringValue(subValue, new []{ key });
        }
    }
}
