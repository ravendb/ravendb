using System.Collections.Generic;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.ETL.Handlers;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class TasksInfoAnalyzer(
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    public TasksAnalysisInfo TasksInfo { get; set; }
    
    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries entries)
    {
        return true;
    }
}
