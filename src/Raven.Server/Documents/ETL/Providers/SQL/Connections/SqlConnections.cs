using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL.Connections
{
    public class SqlConnections // TODO arek - delete it - it's here just because fixing the studio compilation wasn't easy for me
    {
        public string Id { get; set; }

        public readonly Dictionary<string, PredefinedSqlConnection> Connections;

        public SqlConnections()
        {
            Connections = new Dictionary<string, PredefinedSqlConnection>(StringComparer.OrdinalIgnoreCase);
        }
    }
}