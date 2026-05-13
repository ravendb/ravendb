using System;
using Raven.Server.SqlMigration.MsSQL;
using Raven.Server.SqlMigration.MySQL;
using Raven.Server.SqlMigration.NpgSQL;
using Raven.Server.SqlMigration.Oracle;

namespace Raven.Server.SqlMigration
{
    public static class DatabaseDriverDispatcher
    {
        public static IDatabaseDriver CreateDriver(MigrationProvider provider, string connectionString, string[] schemas = null)
        {
            switch (provider)
            {
                case MigrationProvider.MsSQL:
                    return new MsSqlDatabaseMigrator(connectionString);
#pragma warning disable CS0618 // Type or member is obsolete
                case MigrationProvider.MySQL_MySql_Data:
#pragma warning restore CS0618 // Type or member is obsolete
                case MigrationProvider.MySQL_MySqlConnector:
                    return new MySqlDatabaseMigrator(connectionString, "MySqlConnector.MySqlConnectorFactory");
                case MigrationProvider.NpgSQL:
                    return new NpgSqlDatabaseMigrator(connectionString, schemas);

                case MigrationProvider.Oracle:
                    return new OracleDatabaseMigrator(connectionString);

                default:
                    throw new InvalidOperationException("Provider " + provider + " is not yet supported");
            }
        }

        /// <summary>
        /// Overload that accepts the ADO.NET factory-name string from
        /// <c>SqlConnectionString.FactoryName</c>. Same supported set as
        /// <c>CdcSinkSourceVerifier.VerifyAsync</c>. Centralises the
        /// factory-name → <see cref="MigrationProvider"/> mapping so the CDC handlers
        /// don't carry their own copy of the switch.
        /// </summary>
        public static IDatabaseDriver CreateDriver(string factoryName, string connectionString, string[] schemas = null)
        {
            return CreateDriver(GetProviderFromFactoryName(factoryName), connectionString, schemas);
        }

        public static MigrationProvider GetProviderFromFactoryName(string factoryName)
        {
            return factoryName switch
            {
                "Npgsql" => MigrationProvider.NpgSQL,
                "System.Data.SqlClient" or "Microsoft.Data.SqlClient" => MigrationProvider.MsSQL,
                "MySqlConnector.MySqlConnectorFactory" => MigrationProvider.MySQL_MySqlConnector,
#pragma warning disable CS0618 // obsolete provider — kept for legacy connection strings
                "MySql.Data.MySqlClient" => MigrationProvider.MySQL_MySql_Data,
#pragma warning restore CS0618
                "Oracle.ManagedDataAccess.Client" => MigrationProvider.Oracle,
                _ => throw new InvalidOperationException(
                    $"No migration provider is registered for ADO.NET factory '{factoryName}'. " +
                    "Supported factories: Npgsql, System.Data.SqlClient, Microsoft.Data.SqlClient, " +
                    "MySql.Data.MySqlClient, MySqlConnector.MySqlConnectorFactory, Oracle.ManagedDataAccess.Client."),
            };
        }
    }
}
