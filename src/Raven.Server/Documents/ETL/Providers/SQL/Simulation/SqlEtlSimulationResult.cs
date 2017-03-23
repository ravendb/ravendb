using System.Collections.Generic;
using Raven.Server.NotificationCenter.Notifications;

namespace Raven.Server.Documents.ETL.Providers.SQL.Simulation
{
    public class SqlEtlSimulationResult
    {
        public List<TableQuerySummary> Summary { get; set; }

        public AlertRaised LastAlert { get; set; }
    }
}