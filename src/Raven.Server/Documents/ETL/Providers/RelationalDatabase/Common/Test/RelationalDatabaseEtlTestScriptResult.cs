using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Test;

public sealed class RelationalDatabaseEtlTestScriptResult : TestEtlScriptResult
{
    public List<TableQuerySummary> Summary { get; set; }

    public List<TaskItemError> ItemLoadErrors { get; set; }

    public List<SlowSqlStatementInfo> SlowSqlWarnings { get; set; }
    
    public TaskProcessError ProcessError { get; set; }
}
