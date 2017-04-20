using System.Linq;

namespace Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters
{
    public class RelationalDatabaseWriterBase
    {
        protected readonly bool IsSqlServerFactoryType;

        private static readonly string[] SqlServerFactoryNames =
        {
            "System.Data.SqlClient",
            "System.Data.SqlServerCe.4.0",
            "MySql.Data.MySqlClient",
            "System.Data.SqlServerCe.3.5"
        };

        public RelationalDatabaseWriterBase(SqlEtlConnection connection)
        {
            if (SqlServerFactoryNames.Contains(connection.FactoryName))
            {
                IsSqlServerFactoryType = true;
            }
        }
    }
}