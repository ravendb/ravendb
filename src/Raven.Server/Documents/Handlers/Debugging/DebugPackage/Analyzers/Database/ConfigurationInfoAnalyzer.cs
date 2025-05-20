using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class ConfigurationInfoAnalyzer(
    ServerAnalysisInfo serverAnalysisInfo,
    ClusterAnalysisInfo clusterAnalysisInfo,
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    
    public DatabaseSettingsAnalysisInfo SettingsInfo { get; set; }
    
    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries entries)
    {
        if (entries.TryGetEntry<AdminConfigurationHandler>(x => x.GetSettings(), out var settingsEntry) == false)
        {
            AddWarning("Could not retrieve database settings");
            return false;
        }

        SettingsInfo = new DatabaseSettingsAnalysisInfo()
        {
            Settings = new Dictionary<string, ConfigurationEntrySingleValue>(),
            SettingsEntry = settingsEntry
        };
        
        if (settingsEntry.TryGetJsonValue(nameof(SettingsResult.Settings), out List<ConfigurationServerOrDatabaseValue> settings))
        {
            foreach (ConfigurationServerOrDatabaseValue setting in settings)
            {
                if (setting.Metadata.Keys.Contains(RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)))
                {
                    if (clusterAnalysisInfo.ElectionTimeoutInMs == null && clusterAnalysisInfo.DefaultElectionTimeoutInMs == null)
                    {
                        // we get it from a _database_ setting, so we process it just once and store the value in the cluster analysis info

                        if (long.TryParse(setting.Metadata.DefaultValue, out var defaultElectionTimeout))
                        {
                            clusterAnalysisInfo.DefaultElectionTimeoutInMs = defaultElectionTimeout;
                        }                        
                        
                        if (setting.ServerValues is { Count: > 0 })
                        {
                            var entry = setting.ServerValues.First().Value;
                            
                            if (long.TryParse(entry.Value, out var electionTimeout))
                            {
                                clusterAnalysisInfo.ElectionTimeoutInMs = electionTimeout;
                            }
                        }
                        else
                            clusterAnalysisInfo.ElectionTimeoutInMs = defaultElectionTimeout;
                    }
                }
                
                if (setting.ServerValues is { Count: > 0 })
                {
                    foreach (KeyValuePair<string, ConfigurationEntrySingleValue> entry in setting.ServerValues)
                    {
                        if (serverAnalysisInfo.ServerSettings.TryAdd(entry.Key, entry.Value))
                        {
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
                            SettingsInfo.Settings.TryAdd(entry.Key, entry.Value);
                    }
                }
            }
        }

        return true;
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
