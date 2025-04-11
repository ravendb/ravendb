using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class NetworkInfoAnalyzer : AbstractDebugPackageAnalyzer
{
    public NetworkInfoAnalyzer(DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : base(errors, issues)
    {
    }

    public NetworkAnalysisInfo NetworkInfo { get; set; } = new();

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        if (serverEntries.TryGetValue<AdminTcpConnectionDebugInfoHandler, int>(x => x.ActiveConnections(),
                nameof(AdminTcpConnectionDebugInfoHandler.ActiveConnectionsResult.TotalConnections), out var totalActiveConnections))
            NetworkInfo.TotalActiveTcpConnections = totalActiveConnections;

        if (serverEntries.TryGetValue<AdminTcpConnectionDebugInfoHandler, Dictionary<string, TcpConnectionInformation[]>>(
                x => x.ActiveConnections(), nameof(AdminTcpConnectionDebugInfoHandler.ActiveConnectionsResult.Connections),
                out var connectionsByState))
        {
            var connections = new List<TcpConnections>();

            foreach (var item in connectionsByState)
            {
                var state = item.Key;

                var connectionsPerState = new TcpConnections { TcpState = state, NumberOfConnectionsInState = item.Value.Length };

                connectionsPerState.NumberOfConnectionsInState = item.Value.Length;

                var remoteAddresses = item.Value.GroupBy(x =>
                {
                    int lastIndexOf = x.RemoteEndPoint.LastIndexOf(':');

                    if (lastIndexOf == -1)
                        return x.RemoteEndPoint;

                    return x.RemoteEndPoint.Substring(0, lastIndexOf);
                }).ToList();

                connectionsPerState.TopConnectionsInState = remoteAddresses.OrderByDescending(x => x.Count())
                    .Take(5)
                    .ToDictionary(x => x.Key, x => x.Count());

                connections.Add(connectionsPerState);
            }

            NetworkInfo.TcpConnections = connections;
        }

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        var established = NetworkInfo.TcpConnections.Find(x => x.TcpState == nameof(TcpState.Established));
        var timeWait = NetworkInfo.TcpConnections.Find(x => x.TcpState == nameof(TcpState.TimeWait));

        if (established != null)
        {
            if (established.NumberOfConnectionsInState > 1000)
            {
                issues.ServerIssues.Add(
                    new DetectedIssue("High number of established TCP connections",
                        $"There are {established.NumberOfConnectionsInState} established TCP connections in total",
                        IssueSeverity.Warning, IssueCategory.Server)
                    {
                        RecommendedAction = "Please review top remote addresses that are establishing TCP connections to RavenDB server"
                    });
            }

            if (established.TopConnectionsInState.Count > 0)
            {
                var topConnection = established.TopConnectionsInState.OrderByDescending(x => x.Value).FirstOrDefault();

                if (topConnection.Value > 300)
                {
                    issues.ServerIssues.Add(
                        new DetectedIssue("High number of established TCP connections",
                            $"There are {topConnection.Value} established TCP connections coming from IP {topConnection.Key}",
                            IssueSeverity.Error, IssueCategory.Server)
                        {
                            RecommendedAction = "This might be an indication that you are creating a large number of DocumentStore instances. " +
                                                "Are you creating a DocumentStore per request, instead of using DocumentStore as a singleton? ",
                        });
                }
            }
        }

        if (timeWait != null)
        {
            if (timeWait.NumberOfConnectionsInState > 300)
            {
                issues.ServerIssues.Add(
                    new DetectedIssue("High number of TCP connections in TIME_WAIT state",
                        $"There are {timeWait.NumberOfConnectionsInState} TCP connections in TIME_WAIT state",
                        IssueSeverity.Warning, IssueCategory.Server));
            }

            if (established != null && timeWait.NumberOfConnectionsInState > established.NumberOfConnectionsInState / 2)
            {
                issues.ServerIssues.Add(
                    new DetectedIssue("High number of TCP connections in TIME_WAIT state",
                        $"There are {timeWait.NumberOfConnectionsInState} TCP connections in TIME_WAIT state while " +
                        $"the number of ESTABLISHED connections is {established.NumberOfConnectionsInState}. " +
                        "This could indicate a high volume of short-lived connections while it is generally expected " +
                        "to have a stable number of established connections with proper connection reuse.",
                        IssueSeverity.Warning, IssueCategory.Server));
            }
        }
    }

    private class TcpConnectionInformation
    {
        public string LocalEndPoint { get; set; }

        public string RemoteEndPoint { get; set; }
    }
}
