using System;
using System.Data;
using System.Data.SqlClient;

namespace Raven.Server.SqlMigration
{
    public class ConnectionFactory
    {
        public static IDbConnection OpenConnection(string connectionString)
        {
            SqlConnection con;
            
            try
            {
                con = new SqlConnection(connectionString);
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
