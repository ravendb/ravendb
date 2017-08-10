using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using Microsoft.AspNetCore.Server.Kestrel.Internal.Http;

namespace Raven.Server.SqlMigration
{
    class SqlHelper
    {
        public static int QueriesCount = 0;

        public static IDataReader ExecuteReader(IDbCommand cmd)
        {
            QueriesCount++;

            IDataReader reader;
            try
            {
                reader = cmd.ExecuteReader();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to execute query: " + cmd.CommandText, e);
            }

            return reader;
        }

    }
}
