using System;
using System.Data;
using System.Data.SqlClient;

namespace Raven.Server.SqlMigration
{
    public class ConnectionFactory
    {
        public static IDbConnection OpenConnection(string connectionString, string sqlDatabaseName = null)
        {
            SqlConnection con;
            
            try
            {
                con = new SqlConnection(connectionString + (sqlDatabaseName == null ? string.Empty : $";Initial Catalog={sqlDatabaseName}"));
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Cannot create new sql connection using the given connection string", e);
            }

            try
            {
                con.Open();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Cannot open connection using the given connection string", e);
            }

            return con;
        }
    }
}
