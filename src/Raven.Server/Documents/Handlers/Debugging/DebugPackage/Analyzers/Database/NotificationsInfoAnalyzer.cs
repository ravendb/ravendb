using System.Collections.Generic;
using System.Linq;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class NotificationsInfoAnalyzer(
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    private List<DocumentDebugHandler.HugeDocumentInfo> _hugeDocuments;

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries entries)
    {
        if (entries.TryGetValue<DocumentDebugHandler, List<DocumentDebugHandler.HugeDocumentInfo>>(x => x.HugeDocuments(), "Results", out _hugeDocuments) == false)
        {
            AddWarning("Failed to get info about huge documents");
            return false;
        }

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        if (_hugeDocuments is { Count: > 0 })
        {
            var sampleDocs = _hugeDocuments.OrderByDescending(x => x.Size)
                .Take(3)
                .Select(x => $"'{x.Id}' of {new Size(x.Size).HumaneSize} accessed on {x.LastAccess}")
                .ToList();

            issues.ForDatabase(DatabaseName).Add(new DetectedIssue($"Detected {_hugeDocuments.Count} huge documents",
                $"The following sample documents are larger than the configured threshold: {string.Join(", ", sampleDocs)}",
                IssueSeverity.Warning, IssueCategory.Database));
        }
    }
}
