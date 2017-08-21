using System;
using System.Diagnostics;
using System.IO;

namespace Raven.Client.ServerWide.ETL.SQL
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
                    server = GetConnectionStringValue(connectionString, new[] { "Data Source", "Server" });
                    break;
                case SqlProvider.Npgsql:
                    database = GetConnectionStringValue(connectionString, new[] { "Database" });
                    server = GetConnectionStringValue(connectionString, new[] { "Host", "Data Source", "Server" });

                    var port = GetConnectionStringValue(connectionString, new[] { "Port" }, throwIfNotFound: false);

                    if (string.IsNullOrEmpty(port))
                        server += $":{port}";
                    break;
                default:
                    throw new NotSupportedException($"Factory '{factoryName}' is not supported");
            }

            return (database, server);
        }

        public static string GetConnectionStringValue(string connectionString, string[] keyNames, bool throwIfNotFound = true)
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
                throw new InvalidDataException($"Invalid connection string. Could not found neither of '{string.Join(",", keyNames)}' keys in the connection string: {connectionString}");

            return null;
        }
    }
}