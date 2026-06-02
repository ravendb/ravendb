using System.Collections.Generic;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Dashboard;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

public class TasksAnalysisInfo
{
    public DatabaseOngoingTasksInfoItem TaskCounts { get; set; }

    public List<OngoingTaskBackup> BackupTasks { get; set; }
    public BackupInfo LastBackupInfo { get; set; }

    public DebugPackageEntries.Entry OngoingTasksEntry { get; set; }
}
