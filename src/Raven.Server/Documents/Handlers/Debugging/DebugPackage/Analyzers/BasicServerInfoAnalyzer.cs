using System;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.ServerWide;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class BasicServerInfoAnalyzer(DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : AbstractDebugPackageAnalyzer(errors, issues)
{
    public BasicServerInfo BasicServerInfo { get; set; } = new();
    
    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        if (serverEntries.TryGetValue<BuildVersionHandler, string>(x => x.Get(), nameof(BuildNumber.FullVersion), out var fullVersion)) 
            BasicServerInfo.Version = fullVersion;

        if (serverEntries.TryGetValue<RachisAdminHandler, string>(x => x.GetClusterTopology(), nameof(ClusterTopologyResponse.NodeTag), out var nodeTag)) 
            BasicServerInfo.NodeTag = nodeTag;

        if (serverEntries.TryGetValue<ServerInfoHandler, string>(x => x.ServerId(), "ServerId", out var serverId)) 
            BasicServerInfo.ServerId = serverId;
        
        if (serverEntries.TryGetEntry<AdminStatsHandler>(x => x.GetServerStatistics(), out var statsEntry))
        {
            if (statsEntry.TryGetJsonValue<TimeSpan>(nameof(ServerStatistics.UpTime), out var upTime)) 
                BasicServerInfo.UpTime = upTime;
            else
                AddWarning("Could not retrieve server up time");
            
            if (statsEntry.TryGetJsonValue<DateTime>(nameof(ServerStatistics.StartUpTime), out var startUpTime)) 
                BasicServerInfo.StartUpTime = startUpTime;
            else
                AddWarning("Could not retrieve server start up time");
        }

        return true;
    }
}
