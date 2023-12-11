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
    }
}
