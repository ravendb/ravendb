using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Raven.Server.SqlMigration
{
    public class ConnectionFactory
    {
        private string _conStr;
        private bool _hasOpenConnection;

        public ConnectionFactory(string conStr)
        {
            _conStr = conStr;
        }

        public IDbConnection OpenConnection()
        {
            if (_hasOpenConnection)
                throw new InvalidOperationException("You can have only a single database connection open");

            SqlConnection con;

            try
            {
                con = new SqlConnection(_conStr);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot create new sql connection using the current connection string", e);
            }

            con.StateChange += Con_StateChange;

            try
            {
                con.Open();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot open connection using the current connection string", e);
            }

            _hasOpenConnection = true;
            return con;
        }

        private void Con_StateChange(object sender, StateChangeEventArgs e)
        {
            if (e.CurrentState == ConnectionState.Closed)
                _hasOpenConnection = false;
        }
    }
}
