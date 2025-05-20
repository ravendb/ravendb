using System;
using System.Collections.Generic;
using System.Text.Json;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Voron;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class TransactionInfoAnalyzer(
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    private List<TransactionDebugHandler.TransactionInfo> _activeStorageTransactions;

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries entries)
    {
        if (entries.TryGetValue<TransactionDebugHandler, List<TransactionDebugHandler.TransactionInfo>>(x => x.TxInfo(), "tx-info",
                out _activeStorageTransactions))
        {
            foreach (var activeStorageTransaction in _activeStorageTransactions)
            {
                foreach (var tx in activeStorageTransaction.Information)
                {
                }
            }
        }

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        if (_activeStorageTransactions != null)
        {
            foreach (var transactionInfo in _activeStorageTransactions)
            {
                foreach (var tx in transactionInfo.Information)
                {
                    var txDurationInMs = double.Parse(tx.TotalTime.Replace("mSecs", string.Empty));

                    var txDuration = TimeSpan.FromMilliseconds(txDurationInMs);

                    if (txDuration > TimeSpan.FromHours(1))
                    {
                        issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                            $"Long running transaction detected in database '{DatabaseName}'. ",
                            $"{(tx.Flags == TransactionFlags.ReadWrite ? "Write" : "Read")} transaction opened by '{tx.ThreadName}' thread is active for {txDuration:g}",
                            IssueSeverity.Warning, IssueCategory.Database));
                    }
                }
            }
        }
    }
}
