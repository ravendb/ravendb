using System.Linq;

namespace Raven.Server.Documents.SqlReplication
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

        public RelationalDatabaseWriterBase(PredefinedSqlConnection predefinedSqlConnection)
        {
            if (SqlServerFactoryNames.Contains(predefinedSqlConnection.FactoryName))
            {
                IsSqlServerFactoryType = true;
            }
        }
    }
}