using System;
using System.Collections.Generic;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Voron;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class TransactionInfoAnalyzer(
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageAnalyzer(errors, issues)
{
    private List<TransactionDebugHandler.TransactionInfo> _activeStorageTransactions;

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        if (serverEntries.TryGetValue<ServerTransactionDebugHandler, List<TransactionDebugHandler.TransactionInfo>>(x => x.TxInfo(), "tx-info",
                out _activeStorageTransactions) == false)
        {
            AddWarning("Failed to get active transactions");
            return false;
        }

        return true;
    }
    
    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        if (_activeStorageTransactions != null)
        {
            DetectLongRunningTransaction(_activeStorageTransactions, "Long running transaction detected in System storage", issues.ServerIssues, IssueCategory.Server);
        }
    }

    internal static void DetectLongRunningTransaction(List<TransactionDebugHandler.TransactionInfo> transactions, string issueTitle, List<DetectedIssue> issues, IssueCategory category)
    {
        foreach (var transactionInfo in transactions)
        {
            foreach (var tx in transactionInfo.Information)
            {
                var txDurationInMs = double.Parse(tx.TotalTime.TrimEnd(TransactionDebugHandler.TotalTimeMSecondsSuffix));

                var txDuration = TimeSpan.FromMilliseconds(txDurationInMs);

                if (txDuration > TimeSpan.FromHours(1))
                {
                    issues.Add(new DetectedIssue(
                        issueTitle,
                        $"{(tx.Flags == TransactionFlags.ReadWrite ? "Write" : "Read")} transaction opened by '{tx.ThreadName}' thread is active for {txDuration:g}",
                        IssueSeverity.Warning, category));
                }
            }
        }
    }
}
