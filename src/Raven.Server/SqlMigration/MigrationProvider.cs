using System;

namespace Raven.Server.SqlMigration
{
    public enum MigrationProvider
    {
        MsSQL,
        [Obsolete("Unsupported provider. Use 'MySQL_MySqlConnector' instead.")]
        MySQL_MySql_Data,
        MySQL_MySqlConnector,
        NpgSQL,
        Oracle
    }
}
