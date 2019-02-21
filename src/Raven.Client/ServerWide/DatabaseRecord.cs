using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Exceptions.Documents.Indexes;

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

        public DatabaseStateStatus DatabaseState;

        public DatabaseTopology Topology;

        // public OnGoingTasks tasks;  tasks for this node..
        // list backup.. list sub .. list etl.. list repl(watchers).. list sql

        public ConflictSolver ConflictSolverConfig;

        public Dictionary<string, SorterDefinition> Sorters;

        public Dictionary<string, IndexDefinition> Indexes;

        public Dictionary<string, AutoIndexDefinition> AutoIndexes;

        public Dictionary<string, string> Settings = new Dictionary<string, string>();

        public RevisionsConfiguration Revisions;

        public ExpirationConfiguration Expiration;

        public List<PeriodicBackupConfiguration> PeriodicBackups;

        public List<ExternalReplication> ExternalReplications = new List<ExternalReplication>();

        public List<PullReplicationAsSink> SinkPullReplications = new List<PullReplicationAsSink>();

        public List<PullReplicationDefinition> HubPullReplications = new List<PullReplicationDefinition>();

        public Dictionary<string, RavenConnectionString> RavenConnectionStrings = new Dictionary<string, RavenConnectionString>();

        public Dictionary<string, SqlConnectionString> SqlConnectionStrings = new Dictionary<string, SqlConnectionString>();

        public List<RavenEtlConfiguration> RavenEtls;

        public List<SqlEtlConfiguration> SqlEtls;

        public ClientConfiguration Client;

        public StudioConfiguration Studio;

        public long TruncatedClusterTransactionCommandsCount;

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

        public void AddIndex(IndexDefinition definition)
        {
            var lockMode = IndexLockMode.Unlock;

            if (Indexes.TryGetValue(definition.Name, out var existingDefinition))
            {
                if (existingDefinition.LockMode != null)
                    lockMode = existingDefinition.LockMode.Value;

                var result = existingDefinition.Compare(definition);
                if (result != IndexDefinitionCompareDifferences.All)
                {
                    if (result == IndexDefinitionCompareDifferences.LockMode &&
                        definition.LockMode == null)
                        return;

                    if (result == IndexDefinitionCompareDifferences.None)
                        return;
                }
            }

            if (lockMode == IndexLockMode.LockedIgnore)
                return;

            if (lockMode == IndexLockMode.LockedError)
            {
                throw new IndexAlreadyExistException($"Cannot edit existing index {definition.Name} with lock mode {lockMode}");
            }

            Indexes[definition.Name] = definition;
        }

        public void AddIndex(AutoIndexDefinition definition)
        {
            if (AutoIndexes.TryGetValue(definition.Name, out AutoIndexDefinition existingDefinition))
            {
                var result = existingDefinition.Compare(definition);

                if (result == IndexDefinitionCompareDifferences.None)
                    return;

                result &= ~IndexDefinitionCompareDifferences.Priority;
                result &= ~IndexDefinitionCompareDifferences.State;

                if (result != IndexDefinitionCompareDifferences.None)
                    throw new NotSupportedException($"Can not update auto-index: {definition.Name} (compare result: {result})");
            }

            AutoIndexes[definition.Name] = definition;
        }

        public void DeleteIndex(string name)
        {
            Indexes?.Remove(name);
            AutoIndexes?.Remove(name);
        }

        public void DeletePeriodicBackupConfiguration(long backupTaskId)
        {
            Debug.Assert(backupTaskId != 0);

            foreach (var periodicBackup in PeriodicBackups)
            {
                if (periodicBackup.TaskId == backupTaskId)
                {
                    PeriodicBackups.Remove(periodicBackup);
                    break;
                }
            }
        }

        public void EnsureTaskNameIsNotUsed(string taskName)
        {
            if (string.IsNullOrEmpty(taskName))
                throw new ArgumentException("Can't validate task's name because the provided task name is null or empty.");

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
            if (PeriodicBackups.Any(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Can't use task name '{taskName}', there is already a Periodic Backup task with that name");
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
}
