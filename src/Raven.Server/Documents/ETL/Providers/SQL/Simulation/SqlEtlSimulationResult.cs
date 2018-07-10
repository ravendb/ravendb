using System.Collections.Generic;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.Documents.ETL.Providers.SQL.Simulation
{
    public class SqlEtlSimulationResult
    {
        public List<TableQuerySummary> Summary { get; set; }

        public EtlErrorsDetails TransformationErrors { get; set; }

        public EtlErrorsDetails LastLoadErrors{ get; set; }

        public SlowSqlDetails SlowSqlWarnings { get; set; }
    }
}
