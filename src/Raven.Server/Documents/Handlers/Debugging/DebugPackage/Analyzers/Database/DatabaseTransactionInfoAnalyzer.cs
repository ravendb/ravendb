using System.Collections.Generic;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class DatabaseTransactionInfoAnalyzer(
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    private List<TransactionDebugHandler.TransactionInfo> _activeStorageTransactions;

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries entries)
    {
        if (entries.TryGetValue<TransactionDebugHandler, List<TransactionDebugHandler.TransactionInfo>>(x => x.TxInfo(), "tx-info",
                out _activeStorageTransactions) == false)
        {
            AddWarning("Failed to get active transactions");
        }

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        TransactionInfoAnalyzer.DetectLongRunningTransaction(_activeStorageTransactions, $"Long running transaction detected in '{DatabaseName}' database",
            issues.ForDatabase(DatabaseName));
    }
}
