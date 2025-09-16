using System.Collections.Generic;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageDatabaseReport(string databaseName)
{
    public string DatabaseName { get; } = databaseName;
    public DatabaseAnalysisInfo DatabaseInfo { get; set; }
    public DatabaseSettingsAnalysisInfo Settings { get; set; }
    public IndexesAnalysisInfo IndexesInfo { get; set; }
    public TasksAnalysisInfo TasksInfo { get; set; }
}
