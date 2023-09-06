using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.Integrations;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide
{
    public class DatabaseRecordWithEtag : DatabaseRecord
    {
        public long Etag { get; set; }
    }

    // The DatabaseRecord resides in EVERY server/node inside the cluster regardless if the db is actually within the node 
    public class DatabaseRecord
    {
        public DatabaseRecord()
        {
        }

        public DatabaseRecord(string databaseName)
        {
            DatabaseName = databaseName.Trim();
        }

        public string DatabaseName;

        public bool Disabled;

        public bool Encrypted;

        public long EtagForBackup;

        public Dictionary<string, DeletionInProgressStatus> DeletionInProgress;

        public Dictionary<string, RollingIndex> RollingIndexes;

        public DatabaseStateStatus DatabaseState;

        public DatabaseLockMode LockMode;

        public DatabaseTopology Topology;

        // public OnGoingTasks tasks;  tasks for this node..
        // list backup.. list sub .. list etl.. list repl(watchers).. list sql

        public ConflictSolver ConflictSolverConfig;

        public DocumentsCompressionConfiguration DocumentsCompression;

        public Dictionary<string, SorterDefinition> Sorters = new Dictionary<string, SorterDefinition>();

        public Dictionary<string, AnalyzerDefinition> Analyzers = new Dictionary<string, AnalyzerDefinition>();

        public Dictionary<string, IndexDefinition> Indexes;

        public Dictionary<string, List<IndexHistoryEntry>> IndexesHistory;

        public Dictionary<string, AutoIndexDefinition> AutoIndexes;

        public Dictionary<string, string> Settings = new Dictionary<string, string>();

        public RevisionsConfiguration Revisions;

        public TimeSeriesConfiguration TimeSeries;

        public RevisionsCollectionConfiguration RevisionsForConflicts;

        public ExpirationConfiguration Expiration;

        public RefreshConfiguration Refresh;

        public IntegrationConfigurations Integrations;

        public List<PeriodicBackupConfiguration> PeriodicBackups = new List<PeriodicBackupConfiguration>();

        public List<ExternalReplication> ExternalReplications = new List<ExternalReplication>();

        public List<PullReplicationAsSink> SinkPullReplications = new List<PullReplicationAsSink>();

        public List<PullReplicationDefinition> HubPullReplications = new List<PullReplicationDefinition>();

        public Dictionary<string, RavenConnectionString> RavenConnectionStrings = new Dictionary<string, RavenConnectionString>();

        public Dictionary<string, SqlConnectionString> SqlConnectionStrings = new Dictionary<string, SqlConnectionString>();
        
        public Dictionary<string, OlapConnectionString> OlapConnectionStrings = new Dictionary<string, OlapConnectionString>();

        public Dictionary<string, ElasticSearchConnectionString> ElasticSearchConnectionStrings = new Dictionary<string, ElasticSearchConnectionString>();
        
        public Dictionary<string, QueueConnectionString> QueueConnectionStrings = new Dictionary<string, QueueConnectionString>();

        public List<RavenEtlConfiguration> RavenEtls = new List<RavenEtlConfiguration>();

        public List<SqlEtlConfiguration> SqlEtls = new List<SqlEtlConfiguration>();
        
        public List<ElasticSearchEtlConfiguration> ElasticSearchEtls = new List<ElasticSearchEtlConfiguration>();

        public List<OlapEtlConfiguration> OlapEtls = new List<OlapEtlConfiguration>();
        
        public List<QueueEtlConfiguration> QueueEtls = new List<QueueEtlConfiguration>();

        public ClientConfiguration Client;

        public StudioConfiguration Studio;

        public long TruncatedClusterTransactionCommandsCount;

        public HashSet<string> UnusedDatabaseIds = new HashSet<string>();

        [ForceJsonSerialization]
        internal IReadOnlyList<string> SupportedFeatures;

        public void AddSorter(SorterDefinition definition)
        {
            if (Sorters == null)
                Sorters = new Dictionary<string, SorterDefinition>(StringComparer.OrdinalIgnoreCase);

            Sorters[definition.Name] = definition;
        }

        public void DeleteSorter(string sorterName)
        {
            Sorters?.Remove(sorterName);
        }

        public void AddAnalyzer(AnalyzerDefinition definition)
        {
            if (Analyzers == null)
                Analyzers = new Dictionary<string, AnalyzerDefinition>(StringComparer.OrdinalIgnoreCase);

            Analyzers[definition.Name] = definition;
        }

        public void DeleteAnalyzer(string sorterName)
        {
            Analyzers?.Remove(sorterName);
        }

        public void AddIndex(IndexDefinition definition, string source, DateTime createdAt, long raftIndex, int revisionsToKeep, IndexDeploymentMode globalDeploymentMode)
        {
            var lockMode = IndexLockMode.Unlock;

            IndexDefinitionCompareDifferences? differences = null;

            if (Indexes.TryGetValue(definition.Name, out var existingDefinition))
            {
                if (existingDefinition.LockMode != null)
                    lockMode = existingDefinition.LockMode.Value;

                differences = existingDefinition.Compare(definition);
                if (differences != IndexDefinitionCompareDifferences.All)
                {
                    if (differences == IndexDefinitionCompareDifferences.LockMode &&
                        definition.LockMode == null)
                        return;

                    if (differences == IndexDefinitionCompareDifferences.None)
                        return;
                }
            }

            if (lockMode == IndexLockMode.LockedIgnore)
                return;

            if (lockMode == IndexLockMode.LockedError)
            {
                throw new IndexAlreadyExistException($"Cannot edit existing index {definition.Name} with lock mode {lockMode}");
            }

            if (definition.OutputReduceToCollection != null)
            {
                long? version = raftIndex;

                if (differences != null)
                {
                    if (differences.Value.HasFlag(IndexDefinitionCompareDifferences.Maps) == false && 
                        differences.Value.HasFlag(IndexDefinitionCompareDifferences.Reduce) == false &&
                        differences.Value.HasFlag(IndexDefinitionCompareDifferences.Fields) == false &&
                        differences.Value.HasFlag(IndexDefinitionCompareDifferences.AdditionalSources) == false &&
                        differences.Value.HasFlag(IndexDefinitionCompareDifferences.AdditionalAssemblies) == false)
                    {
                        // index definition change does not affect the output documents - version need to stay the same

                        version = existingDefinition.ReduceOutputIndex;
                    }
                }

                definition.ReduceOutputIndex = version;
            }

            Indexes[definition.Name] = definition;
            var isRolling = IsRolling(definition.DeploymentMode, globalDeploymentMode);
            
            AddIndexHistory(definition, source, revisionsToKeep, createdAt);
            
            if (isRolling)
            {
                definition.ClusterState ??= new ClusterState();
                definition.ClusterState.LastRollingDeploymentIndex = raftIndex;
                if (differences == null || (differences.Value & IndexDefinition.ReIndexRequiredMask) != 0)
                {
                    InitializeRollingDeployment(definition.Name, createdAt, raftIndex);
                    definition.DeploymentMode = IndexDeploymentMode.Rolling;
                }
            }
        }

        internal void AddIndexHistory(IndexDefinition definition, string source, int revisionsToKeep, DateTime createdAt, Dictionary<string, RollingIndexDeployment> rollingIndexDeployment = null, bool isFromCommand = false, bool isRolling = false)
        {
            IndexesHistory ??= new();
            List<IndexHistoryEntry> history;
            if (IndexesHistory.TryGetValue(definition.Name, out history) == false)
            {
                history = new List<IndexHistoryEntry>();
                IndexesHistory.Add(definition.Name, history);
            }
            
            bool isNewIndexHistory = true;
            if (history.Count > 0)
            {
                var lastEntry = history[0];
                var sameDefinition = definition.Compare(lastEntry.Definition) == IndexDefinitionCompareDifferences.None;
                var currentIsFromSmuggler = source == "Smuggler";
                var isCreatedBeforeCurrent = createdAt.CompareTo(lastEntry.CreatedAt) > 0;
                isNewIndexHistory = isRolling == false && (sameDefinition && currentIsFromSmuggler && isCreatedBeforeCurrent) == false;
            }
            
            if (isNewIndexHistory)
            {
                var ihe = new IndexHistoryEntry {Definition = definition, CreatedAt = createdAt, Source = source};
                if (isFromCommand)
                    history.Add(ihe);
                else
                    history.Insert(0, ihe);
            }
            else
            {
                var latestVersion = history[0];
                latestVersion.CreatedAt = createdAt;
                latestVersion.Source = source;
                latestVersion.RollingDeployment = rollingIndexDeployment;
            }

            if (history.Count > revisionsToKeep)
            {
                history.RemoveRange(revisionsToKeep, history.Count - revisionsToKeep);
            }
        }

        public void AddIndex(AutoIndexDefinition definition)
        {
            AddIndex(definition, SystemTime.UtcNow, 0, globalDeploymentMode: IndexDeploymentMode.Parallel);
        }

        internal void AddIndex(AutoIndexDefinition definition, DateTime createdAt, long raftIndex, IndexDeploymentMode globalDeploymentMode)
        {
            IndexDefinitionCompareDifferences? differences = null;

            if (AutoIndexes.TryGetValue(definition.Name, out AutoIndexDefinition existingDefinition))
            {
                differences = existingDefinition.Compare(definition);

                if (differences == IndexDefinitionCompareDifferences.None)
                    return;

                differences &= ~IndexDefinitionCompareDifferences.Priority;
                differences &= ~IndexDefinitionCompareDifferences.State;

                if (differences != IndexDefinitionCompareDifferences.None)
                    throw new NotSupportedException($"Can not update auto-index: {definition.Name} (compare result: {differences})");
            }

            AutoIndexes[definition.Name] = definition;
            
            if (globalDeploymentMode == IndexDeploymentMode.Rolling)
            {
                if (differences == null || (differences.Value & IndexDefinition.ReIndexRequiredMask) != 0)
                    InitializeRollingDeployment(definition.Name, createdAt, raftIndex);
            }
        }

        internal static bool IsRolling(IndexDeploymentMode? fromDefinition, IndexDeploymentMode fromSetting)
        {
            if (fromDefinition.HasValue == false)
                return fromSetting == IndexDeploymentMode.Rolling;

            return fromDefinition == IndexDeploymentMode.Rolling;
        }

        private void InitializeRollingDeployment(string indexName, DateTime createdAt, long raftIndex)
        {
            RollingIndexes ??= new Dictionary<string, RollingIndex>();
            
            var rollingIndex = new RollingIndex();
            RollingIndexes[indexName] = rollingIndex;

            var chosenNode = ChooseFirstNode();

            foreach (var node in Topology.AllNodes)
            {
                var deployment = new RollingIndexDeployment
                {
                    CreatedAt = createdAt
                };

                if (node.Equals(chosenNode))
                {
                    deployment.State = RollingIndexState.Running;
                    deployment.StartedAt = createdAt;
                }
                else
                {
                    deployment.State = RollingIndexState.Pending;
                }

                rollingIndex.ActiveDeployments[node] = deployment;
                rollingIndex.RaftCommandIndex = raftIndex;
            }
        }

        private string ChooseFirstNode()
        {
            string chosenNode;

            if (Topology.Members.Count > 0)
            {
                chosenNode = Topology.Members.Last();
            }
            else if (Topology.Promotables.Count > 0)
            {
                chosenNode = Topology.Promotables.Last();
            }
            else
            {
                chosenNode = Topology.Rehabs.Last();
            }

            return chosenNode;
        }

        public void DeleteIndex(string name)
        {
            Indexes?.Remove(name);
            AutoIndexes?.Remove(name);
            IndexesHistory?.Remove(name);
            RollingIndexes?.Remove(name);
        }

        public PeriodicBackupConfiguration DeletePeriodicBackupConfiguration(long backupTaskId)
        {
            Debug.Assert(backupTaskId != 0);

            foreach (var periodicBackup in PeriodicBackups)
            {
                if (periodicBackup.TaskId == backupTaskId)
                {
                    if (periodicBackup.Name != null && 
                        periodicBackup.Name.StartsWith(ServerWideBackupConfiguration.NamePrefix, StringComparison.OrdinalIgnoreCase))
                        throw new InvalidOperationException($"Can't delete task id: {periodicBackup.TaskId}, name: '{periodicBackup.Name}', " +
                                                            $"because it is a server-wide backup task. Please use a dedicated operation.");

                    PeriodicBackups.Remove(periodicBackup);
                    return periodicBackup;
                }
            }

            return null;
        }

        public void EnsureTaskNameIsNotUsed(string taskName)
        {
            if (string.IsNullOrEmpty(taskName))
                throw new ArgumentException("Can't validate task's name because the provided task name is null or empty.");

            if (taskName.StartsWith(ServerWideBackupConfiguration.NamePrefix, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Task name '{taskName}' cannot start with: {ServerWideBackupConfiguration.NamePrefix} because it's a prefix for server-wide backup tasks");

            if (ExternalReplications.Any(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Can't use task name '{taskName}', there is already an External Replications task with that name");
            if (SinkPullReplications.Any(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Can't use task name '{taskName}', there is already a Sink Pull Replications task with that name");
            if (HubPullReplications.Any(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Can't use task name '{taskName}', there is already a Hub Pull Replications with that name");
            if (RavenEtls.Any(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Can't use task name '{taskName}', there is already an ETL task with that name");
            if (SqlEtls.Any(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Can't use task name '{taskName}', there is already a SQL ETL task with that name");
            if (OlapEtls.Any(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Can't use task name '{taskName}', there is already an OLAP ETL task with that name");
            if (PeriodicBackups.Any(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Can't use task name '{taskName}', there is already a Periodic Backup task with that name");
            if (QueueEtls.Any(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Can't use task name '{taskName}', there is already a Queue ETL task with that name");
        }

        internal string EnsureUniqueTaskName(string defaultTaskName)
        {
            var result = defaultTaskName;

            int counter = 2;

            while (true)
            {
                try
                {
                    EnsureTaskNameIsNotUsed(result);

                    return result;
                }
                catch (Exception)
                {
                    result = $"{defaultTaskName} #{counter}";
                    counter++;
                }
            }
        }

        public int GetIndexesCount()
        {
            var count = 0;

            if (Indexes != null)
                count += Indexes.Count;

            if (AutoIndexes != null)
                count += AutoIndexes.Count;

            return count;
        }
    }

    public class IndexHistoryEntry
    {
        public IndexDefinition Definition { get; set; }
        
        public string Source { get; set; }
        
        public DateTime CreatedAt { get; set; }
        
        public Dictionary<string, RollingIndexDeployment> RollingDeployment { get; set; }
    }

    public enum DatabaseLockMode
    {
        Unlock,
        PreventDeletesIgnore,
        PreventDeletesError
    }

    public enum DatabaseStateStatus
    {
        Normal,
        RestoreInProgress
    }

    public enum DeletionInProgressStatus
    {
        No,
        SoftDelete,
        HardDelete
    }

    public class DocumentsCompressionConfiguration : IDynamicJson
    {
        public string[] Collections { get; set; }
        public bool CompressAllCollections { get; set; }
        public bool CompressRevisions { get; set; }

        public DocumentsCompressionConfiguration()
        {
        }

        public DocumentsCompressionConfiguration(bool compressRevisions, params string[] collections)
        {
            Collections = collections ?? throw new ArgumentNullException(nameof(collections));
            CompressRevisions = compressRevisions;
        }

        public DocumentsCompressionConfiguration(bool compressRevisions, bool compressAllCollections, params string[] collections)
        {
            Collections = collections ?? throw new ArgumentNullException(nameof(collections));
            CompressAllCollections = compressAllCollections;
            CompressRevisions = compressRevisions;
        }

        protected bool Equals(DocumentsCompressionConfiguration other)
        {
            var mine = new HashSet<string>(Collections,StringComparer.OrdinalIgnoreCase);
            var them = new HashSet<string>(other.Collections, StringComparer.OrdinalIgnoreCase);
            return CompressRevisions == other.CompressRevisions && 
                   CompressAllCollections == other.CompressAllCollections &&
                   mine.SetEquals(them);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((DocumentsCompressionConfiguration)obj);
        }

        public override int GetHashCode()
        {
            int hash = Collections.Length;

            foreach (string collection in Collections)
            {
                hash = 31 * hash + collection.GetHashCode();
            }

            hash = 31 * hash + CompressRevisions.GetHashCode();
            hash = 31 * hash + CompressAllCollections.GetHashCode();
            return hash;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Collections)] = new DynamicJsonArray(Collections),
                [nameof(CompressAllCollections)] = CompressAllCollections,
                [nameof(CompressRevisions)] = CompressRevisions
            };
        }
    }
}
