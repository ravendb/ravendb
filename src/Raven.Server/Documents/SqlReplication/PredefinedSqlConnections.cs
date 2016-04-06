using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.SqlReplication
{
    public class PredefinedSqlConnections
    {
        public readonly Dictionary<string, PredefinedSqlConnection> Connections;

        public PredefinedSqlConnections()
        {
            Connections = new Dictionary<string, PredefinedSqlConnection>(StringComparer.OrdinalIgnoreCase);
        }
    }
}