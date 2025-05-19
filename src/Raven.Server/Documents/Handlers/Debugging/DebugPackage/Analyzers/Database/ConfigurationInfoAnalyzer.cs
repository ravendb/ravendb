using System;
using System.Collections.Generic;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class ConfigurationInfoAnalyzer(
    ServerAnalysisInfo serverAnalysisInfo,
    ClusterAnalysisInfo clusterAnalysisInfo,
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    private ConfigurationEntrySingleValue _customizedElectionTimeoutSetting = null;
    public Dictionary<string, ConfigurationEntrySingleValue> DatabaseSettings { get; set; } = new();

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries entries)
    {
        if (entries.TryGetEntry<AdminConfigurationHandler>(x => x.GetSettings(), out var settingsEntry) == false)
        {
            AddWarning("Could not retrieve database settings");
            return false;
        }

        if (settingsEntry.TryGetJsonValue(nameof(SettingsResult.Settings), out List<ConfigurationServerOrDatabaseValue> settings))
        {
            foreach (ConfigurationServerOrDatabaseValue setting in settings)
            {
                if (setting.ServerValues is { Count: > 0 })
                {
                    foreach (KeyValuePair<string, ConfigurationEntrySingleValue> entry in setting.ServerValues)
                    {
                        if (serverAnalysisInfo.ServerSettings.TryAdd(entry.Key, entry.Value))
                        {
                            if (entry.Key.Equals(RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout),
                                    StringComparison.OrdinalIgnoreCase) && entry.Value.HasValue)
                            {
                                // got customized Cluster.ElectionTimeoutInMs setting

                                if (clusterAnalysisInfo.ElectionTimeout == null)
                                {
                                    // we get it from a database setting, so we process it just once and store the value in the cluster analysis info

                                    if (TimeSpan.TryParse(entry.Value.Value, out var electionTimeout))
                                    {
                                        clusterAnalysisInfo.ElectionTimeout = electionTimeout;

                                        if (setting.Metadata.DefaultValue != entry.Value.Value)
                                            _customizedElectionTimeoutSetting = entry.Value;
                                    }
                                }
                            }

                            if (entry.Key.Equals(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)) && 
                                string.IsNullOrEmpty(serverAnalysisInfo.PublicServerUrl))
                            {
                                serverAnalysisInfo.PublicServerUrl = entry.Value.Value;
                            }
                            
                            if (entry.Key.Equals(RavenConfiguration.GetKey(x => x.Core.ServerUrls)) && 
                                string.IsNullOrEmpty(serverAnalysisInfo.ServerUrl))
                            {
                                serverAnalysisInfo.ServerUrl = entry.Value.Value;
                            }
                        }
                    }
                }
                else if (setting.DatabaseValues is { Count: > 0 })
                {
                    foreach (KeyValuePair<string, ConfigurationEntrySingleValue> entry in setting.ServerValues)
                    {
                        if (entry.Value.HasValue && entry.Value.Value != setting.Metadata.DefaultValue)
                            DatabaseSettings.TryAdd(entry.Key, entry.Value);
                    }
                }
            }
        }

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        if (_customizedElectionTimeoutSetting != null)
        {
            // it's ensured we set it only the first time we read this value from a database setting

            issues.ClusterIssues.Add(new DetectedIssue("Custom Election Timeout setting defined",
                $"The setting '{RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)}' is set to " +
                $"non default value: {_customizedElectionTimeoutSetting.Value} ms",
                IssueSeverity.Warning, IssueCategory.Cluster));
        }
    }

    private class ConfigurationServerOrDatabaseValue : ConfigurationEntryValue
    {
        public ConfigurationServerOrDatabaseValue()
        {
            // for deserialization
        }

        public ConfigurationServerOrDatabaseValue(ConfigurationEntryMetadata metadata) : base(metadata)
        {
        }

        public Dictionary<string, ConfigurationEntrySingleValue> ServerValues { get; set; }

        public Dictionary<string, ConfigurationEntrySingleValue> DatabaseValues { get; set; }

        public new ConfigurationEntryMetadata Metadata { get; set; }
    }
}
