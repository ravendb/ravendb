using System.Data.Common;

namespace Raven.Server.SqlMigration
{
    internal abstract class SqlSchemaQueries
    {
        public abstract string SelectColumnsQuery { get; }

        public abstract string SelectPrimaryKeysQuery { get; }

        public abstract string SelectReferentialConstraintsQuery { get; }

        public abstract string SelectKeyColumnUsageQuery { get; }

        protected internal virtual void AddSchemaParameter(DbCommand cmd, DbConnection connection)
        {
        }

        public static (string Schema, string TableName) GetTableNameFromReader(DbDataReader reader)
        {
            return (reader["TABLE_SCHEMA"].ToString(), reader["TABLE_NAME"].ToString());
        }
    }
}
