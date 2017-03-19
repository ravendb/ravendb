using System;
using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.Connections;

namespace Raven.Server.Documents.ETL
{
    public class EtlConfiguration
    {
        public List<RavenEtlConfiguration> RavenTargets = new List<RavenEtlConfiguration>();

        public List<SqlEtlConfiguration> SqlTargets = new List<SqlEtlConfiguration>();

        public Dictionary<string, PredefinedSqlConnection> SqlConnections = new Dictionary<string, PredefinedSqlConnection>(StringComparer.OrdinalIgnoreCase);
    }
}