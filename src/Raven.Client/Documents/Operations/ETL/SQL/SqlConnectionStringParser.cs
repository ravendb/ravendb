using System;
using System.IO;

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

                    var port = GetConnectionStringValue(connectionString, new[] { "Port" });

                    if (string.IsNullOrEmpty(port))
                        server += $":{port}";
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
                var keyValue = part.Split('=');
               
                if (keyValue.Length != 2)
                {
                    continue;
                }

                foreach (var key in keyNames)
                {
                    if (keyValue[0].Trim().Equals(key, StringComparison.OrdinalIgnoreCase))
                        return keyValue[1].Trim();
                }
            }

            return null;
        }
    }
}
