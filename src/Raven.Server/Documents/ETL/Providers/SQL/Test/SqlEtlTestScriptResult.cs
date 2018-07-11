using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.Documents.ETL.Providers.SQL.Test
{
    public class SqlEtlTestScriptResult : TestEtlScriptResult
    {
        public List<TableQuerySummary> Summary { get; set; }

        public List<EtlErrorInfo> LoadErrors{ get; set; }

        public List<SlowSqlStatementInfo> SlowSqlWarnings { get; set; }
    }
}
