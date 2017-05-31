using System;
using System.Diagnostics;
using System.IO;

namespace Raven.Client.Server.ETL
{
    internal class SqlConnectionStringParser
    {
        internal static string GetDatabaseAndServerFromConnectionString(string factoryName, string connectionString)
        {
            string database;
            string server;

            switch (factoryName)
            {
                case "System.Data.SqlClient":
                    database = GetConnectionStringValue(connectionString, new[] { "Initial Catalog", "Database" });
                    server = GetConnectionStringValue(connectionString, new[] { "Data Source", "Server" });
                    break;
                case "Npgsql":
                    database = GetConnectionStringValue(connectionString, new[] { "Database" });
                    server = GetConnectionStringValue(connectionString, new[] { "Host", "Data Source", "Server" });

                    var port = GetConnectionStringValue(connectionString, new[] { "Port" }, throwIfNotFound: false);

                    if (string.IsNullOrEmpty(port))
                        server += $":{port}";
                    break;
                case "System.Data.SqlServerCe.4.0":
                case "System.Data.OleDb":
                case "System.Data.OracleClient":
                case "MySql.Data.MySqlClient":
                case "System.Data.SqlServerCe.3.5":
                    // keep it sync with DbProviderFactories
                    throw new NotImplementedException($"Factory '{factoryName}' is not implemented yet");
                default:
                    throw new NotSupportedException($"Factory '{factoryName}' is not supported");
            }

            return $"{database}@{server}";
        }

        private static string GetConnectionStringValue(string connectionString, string[] keyNames, bool throwIfNotFound = true)
        {
            var parts = connectionString.Split(';');

            foreach (var part in parts)
            {
                var keyValue = part.Split('=');

                Debug.Assert(keyValue.Length == 2);

                foreach (var key in keyNames)
                {
                    if (keyValue[0].Trim().Equals(key, StringComparison.OrdinalIgnoreCase))
                        return keyValue[1].Trim();
                }
            }

            if (throwIfNotFound)
                throw new InvalidDataException($"Could not found neither of '{string.Join(",", keyNames)}' keys in the connection string: {connectionString}");

            return null;
        }
    }
}