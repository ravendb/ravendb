using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public abstract class AbstractDebugPackageDatabaseAnalyzer(string databaseName, DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : AbstractDebugPackageAnalyzer(errors, issues)
{
    protected string DatabaseName { get; } = databaseName;
}
