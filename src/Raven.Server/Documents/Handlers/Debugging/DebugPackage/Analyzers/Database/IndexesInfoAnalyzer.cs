using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Database;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;

public class IndexesInfoAnalyzer(
    GeneralDatabaseInfoAnalyzer generalInfoAnalyzer,
    string databaseName,
    DebugPackageAnalyzeErrors errors,
    DebugPackageAnalysisIssues issues) : AbstractDebugPackageDatabaseAnalyzer(databaseName, errors, issues)
{
    private readonly GeneralDatabaseInfoAnalyzer _generalInfoAnalyzer = generalInfoAnalyzer;
    public IndexesAnalysisInfo IndexesInfo { get; set; }

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries entries)
    {
        IndexDefinition[] definitions = null;
        if (entries.TryGetEntry<IndexHandler>(x => x.GetAll(), out var indexDefinitionsEntry))
        {
            indexDefinitionsEntry.TryGetJsonValue("Results", out definitions);
        }
        else
        {
            AddWarning("Could not retrieve index definitions.");
        }
        
        if (entries.TryGetValue<IndexHandler, IndexStats[]>(x => x.Stats(), "Results", out var stats) == false)
        {
            AddWarning("Could not retrieve indexes stats.");
        }

        if (entries.TryGetValue<IndexHandler, IndexMetadataInfo[]>(x => x.Metadata(), "Results", out var metadata) == false)
        {
            AddWarning("Could not retrieve indexes metadata.");
        }

        IndexErrors[] errors = null;
        if (entries.TryGetEntry<IndexHandler>(x => x.GetErrors(), out var indexErrorsEntry))
        {
            indexErrorsEntry.TryGetJsonValue("Results", out errors);
        }
        else
        {
            AddWarning("Could not retrieve index errors");
        }
        
        if (entries.TryGetEntry<IndexHandler>(x => x.Performance(), out var indexingPerformanceEntry) == false)
        {
            AddWarning("Could not retrieve indexing performance.");
        }

        if (stats == null && metadata == null && errors == null)
            return false;

        IndexesInfo = new IndexesAnalysisInfo
        {
            Definitions = definitions,
            DefinitionsEntry = indexDefinitionsEntry,
            Stats = stats,
            Metadata = metadata,
            Errors = errors,
            ErrorsEntry = indexErrorsEntry,
            PerformanceEntry = indexingPerformanceEntry,
        };

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        if (IndexesInfo.Definitions != null)
        {
            var indexesStoringAllFields = new List<string>();
            
            foreach (var definition in IndexesInfo.Definitions)
            {
                if (definition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out IndexFieldOptions allFieldsValue) &&
                    allFieldsValue.Storage == FieldStorage.Yes)
                {
                    indexesStoringAllFields.Add(definition.Name);
                }
            }

            if (indexesStoringAllFields.Count > 0)
            {
                var description = GetIndexesDescription(indexesStoringAllFields, indexState: null, "storing all fields") 
                                  + ". Storing all index fields might be storage expensive and causes additional index processing cost";

                if (_generalInfoAnalyzer.Analyzed && _generalInfoAnalyzer.DatabaseInfo?.DatabaseRecord?.Encrypted == true)
                {
                    description += " that will result in higher CPU usage due to encryption usage.";
                }
                
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    $"Storing all fields in indexes",
                    description,
                    IssueSeverity.Warning,
                    IssueCategory.Indexes));
            }
        }

        if (IndexesInfo.Stats != null)
        {
            int coraxUsageCount = 0;

            var disabledIndexes = new List<string>();
            var erroredIndexes = new List<string>();
            var idleIndexes = new List<string>();
            var pausedIndexes = new List<string>();
            var staleIndexes = new List<string>();

            foreach (var stats in IndexesInfo.Stats)
            {
                if (stats.State == IndexState.Disabled)
                {
                    disabledIndexes.Add(stats.Name);
                }
                else if (stats.State == IndexState.Error)
                {
                    erroredIndexes.Add(stats.Name);
                }
                else if (stats.State == IndexState.Idle)
                {
                    idleIndexes.Add(stats.Name);
                }

                if (stats.ReferencedCollections?.Count >= 2)
                {
                    issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                        $"Multi LoadDocument() usage in '{stats.Name}' index",
                        $"Index '{stats.Name}' references {stats.ReferencedCollections.Count} collections which can impact performance",
                        stats.ReferencedCollections.Count >= 4 ? IssueSeverity.Warning : IssueSeverity.Info,
                        IssueCategory.Indexes));
                }

                if (stats.Status == IndexRunningStatus.Paused && stats.State != IndexState.Disabled && stats.State != IndexState.Error)
                {
                    pausedIndexes.Add(stats.Name);
                }

                if (stats.SearchEngineType == SearchEngineType.Corax)
                    coraxUsageCount++;

                if (stats.IsStale &&
                    stats.State != IndexState.Disabled && // disabled, errored and paused indexes got already reported - it's known they are stale
                    stats.State != IndexState.Error &&
                    stats.Status != IndexRunningStatus.Paused)
                {
                    if (stats.Collections != null)
                    {
                        foreach (var statsCollection in stats.Collections)
                        {
                            if (statsCollection.Value.DocumentLag < 128 && statsCollection.Value.TombstoneLag < 128) // ignore trivial staleness 
                                continue;
                            
                            staleIndexes.Add(stats.Name);
                            break;
                        }
                    }
                }
                
                if (stats.MaxNumberOfOutputsPerDocument > 10_000)
                {
                    issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                        $"High fanout rate in '{stats.Name}' index ({stats.Type})",
                        $"Index '{stats.Name}' has produced {stats.MaxNumberOfOutputsPerDocument} outputs from a single document. This can have performance implications",
                        IssueSeverity.Warning,
                        IssueCategory.Indexes));
                }
            }

            if (disabledIndexes.Count > 0)
            {
                string description = GetIndexesDescription(disabledIndexes, "disabled");
                
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Disabled indexes",
                    description,
                    IssueSeverity.Warning,
                    IssueCategory.Indexes));
            }
            
            if (erroredIndexes.Count > 0)
            {
                string description = GetIndexesDescription(erroredIndexes, "in Error state");
                
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Errored indexes",
                    description,
                    IssueSeverity.Error,
                    IssueCategory.Indexes));
            }
            
            if (idleIndexes.Count > 0)
            {
                string description = GetIndexesDescription(idleIndexes, "idle");
                
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Idle indexes",
                    description,
                    IssueSeverity.Warning,
                    IssueCategory.Indexes));
            }

            if (pausedIndexes.Count > 0)
            {
                string description = GetIndexesDescription(pausedIndexes, "paused");
                
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Paused indexes",
                    description,
                    IssueSeverity.Warning,
                    IssueCategory.Indexes));
            }

            if (coraxUsageCount > 0)
            {
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Corax is used",
                    $"Corax is used in {(coraxUsageCount == IndexesInfo.Stats.Length ? "all" : coraxUsageCount)} indexes",
                    IssueSeverity.Info,
                    IssueCategory.Indexes));
            }

            if (staleIndexes.Count > 0)
            {
                string description = GetIndexesDescription(staleIndexes, "stale");

                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Stale indexes",
                    description,
                    IssueSeverity.Warning,
                    IssueCategory.Indexes));
            }
        }
        
        if (IndexesInfo.Metadata != null)
        {
            var dynamicFieldsIndexes = new List<string>();
            var compareExchangeIndexes = new List<string>();
            
            foreach (var metadata in IndexesInfo.Metadata)
            {
                if (metadata.HasDynamicFields)
                {
                    dynamicFieldsIndexes.Add(metadata.Name);
                }

                if (metadata.HasCompareExchange)
                {
                    compareExchangeIndexes.Add(metadata.Name);
                }
            }

            if (dynamicFieldsIndexes.Count > 0)
            {
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Dynamic fields usage",
                    GetIndexesDescription(dynamicFieldsIndexes, indexState: null, "that use CreateField() method to define dynamic index fields"),
                    IssueSeverity.Info,
                    IssueCategory.Indexes));
            }
            
            if (compareExchangeIndexes.Count > 0)
            {
                issues.ForDatabase(DatabaseName).Add(new DetectedIssue(
                    "Indexes processing Compare Exchange values",
                    GetIndexesDescription(compareExchangeIndexes, indexState: null, "that reference Compare Exchange values"),
                    IssueSeverity.Info,
                    IssueCategory.Indexes));
            }
        }
    }

    public static void DetectIssuesInDatabaseGroup(Dictionary<string, IndexesAnalysisInfo> indexesInfoPerNode, ref List<DetectedIssue> issues)
    {
        var indexEntriesCountPerNode = new Dictionary<string, Dictionary<string, long>>();

        foreach (var (nodeName, nodeIndexesInfo) in indexesInfoPerNode)
        {
            if (nodeIndexesInfo?.Stats == null)
                continue;

            foreach (var stats in nodeIndexesInfo.Stats)
            {
                if (stats.State != IndexState.Normal)
                {
                    // don't count entries for errored or disabled indexes
                    continue;
                }
                
                if (indexEntriesCountPerNode.TryGetValue(stats.Name, out var entriesCountByNode) == false)
                {
                    entriesCountByNode = new Dictionary<string, long>();
                    indexEntriesCountPerNode[stats.Name] = entriesCountByNode;
                }

                entriesCountByNode[nodeName] = stats.EntriesCount;
            }
        }

        foreach (var (indexName, entriesCountByNode) in indexEntriesCountPerNode)
        {
            if (entriesCountByNode.Count > 1)
            {
                var minCount = entriesCountByNode.Values.Min();
                var maxCount = entriesCountByNode.Values.Max();

                var diff = Math.Abs(maxCount - minCount);
                
                if (minCount != maxCount && (1.0 * diff / maxCount > 0.1 || diff > 10_000))
                {
                    var discrepancies = string.Join(", ", entriesCountByNode.Select(kvp => $"{kvp.Key}: {kvp.Value}"));

                    issues.Add(new DetectedIssue(
                        $"Inconsistent number of entries for '{indexName}' index",
                        $"The number of entries for index '{indexName}' differs across nodes: {discrepancies}",
                        IssueSeverity.Warning,
                        IssueCategory.Indexes
                    ));
                }
            }
        }
    }
    
    private static string GetIndexesDescription(List<string> indexNames, string indexState, string additionalDetails = null)
    {
        string description;
        
        if (indexNames.Count == 1)
            description = $"There is {indexState} index ";
        else
            description = $"There are {indexNames.Count} {indexState} indexes ";

        if (string.IsNullOrEmpty(additionalDetails) == false)
            description += $" {additionalDetails}";
        
        description += ": ";
        
        foreach (var staleIndex in indexNames)
            description += $"{staleIndex}, ";
        
        description = description.TrimEnd(',', ' ');
        
        description = Regex.Replace(description, @"\s+", " ");
    
        return description;

    }
}
