using System;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public abstract class AbstractDebugPackageAnalyzer(DebugPackageAnalyzeErrors analyzeErrors, DebugPackageAnalysisIssues detectedIssues)
{
    public string Name => GetType().Name;

    public bool Analyzed { get; private set; }
    
    public void Analyze(DebugPackageEntries entries)
    {
        var infoRetrieved = RetrieveAnalyzerInfo(entries);

        if (infoRetrieved)
            DetectIssues(detectedIssues);

        Analyzed = true;
    }

    protected void AddError(string message, Exception exception = null)
    {
        analyzeErrors.AddAnalyzerError(Name, message, AnalyzeErrorSeverity.Error, exception);
    }

    protected void AddWarning(string message)
    {
        analyzeErrors.AddAnalyzerError(Name, message, AnalyzeErrorSeverity.Warning);
    }

    protected abstract bool RetrieveAnalyzerInfo(DebugPackageEntries entries);

    protected virtual void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        
    }
}
