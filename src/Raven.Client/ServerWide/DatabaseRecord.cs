using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Expiration;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Client.ServerWide.Revisions;

namespace Raven.Client.ServerWide
{
    // The DatabaseRecord resides in EVERY server/node inside the cluster regardless if the db is actually within the node 
    public class DatabaseRecord
    {
        public DatabaseRecord()
        {
        }

        public DatabaseRecord(string databaseName)
        {
            DatabaseName = databaseName;
        }

        public string DatabaseName;

        public bool Disabled;

        public bool Encrypted;

        public Dictionary<string, DeletionInProgressStatus> DeletionInProgress;

        public string DataDirectory;

        public DatabaseTopology Topology;

        // public OnGoingTasks tasks;  tasks for this node..
        // list backup.. list sub .. list etl.. list repl(watchers).. list sql

        public ConflictSolver ConflictSolverConfig = new ConflictSolver();

        public Dictionary<string, IndexDefinition> Indexes;

        public Dictionary<string, AutoIndexDefinition> AutoIndexes;

        public Dictionary<string, long> Identities;

        public Dictionary<string, string> Settings = new Dictionary<string, string>();

        //todo: see how we can protect this
        public Dictionary<string, string> SecuredSettings;

        public RevisionsConfiguration Revisions;

        public ExpirationConfiguration Expiration;

        public List<PeriodicBackupConfiguration> PeriodicBackups;

        public List<ExternalReplication> ExternalReplication = new List<ExternalReplication>(); // Watcher only receives (slave)

        public Dictionary<string, RavenConnectionString> RavenConnectionStrings = new Dictionary<string, RavenConnectionString>();

        public Dictionary<string, SqlConnectionString> SqlConnectionStrings = new Dictionary<string, SqlConnectionString>();

        public List<RavenEtlConfiguration> RavenEtls;

        public List<SqlEtlConfiguration> SqlEtls;

        public ClientConfiguration Client;

        public string CustomFunctions;

        public void AddIndex(IndexDefinition definition)
        {
            var lockMode = IndexLockMode.Unlock;

            IndexDefinition existingDefinition;
            if (Indexes.TryGetValue(definition.Name, out existingDefinition))
            {
                if (existingDefinition.LockMode != null)
                    lockMode = existingDefinition.LockMode.Value;

                var result = existingDefinition.Compare(definition);
                if (result != IndexDefinitionCompareDifferences.All)
                {
                    result &= ~IndexDefinitionCompareDifferences.Etag;

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
                throw new IndexOrTransformerAlreadyExistException($"Cannot edit existing index {definition.Name} with lock mode {lockMode}");
            }

            Indexes[definition.Name] = definition;
        }

        public void AddIndex(AutoIndexDefinition definition)
        {
            if (AutoIndexes.TryGetValue(definition.Name, out AutoIndexDefinition existingDefinition))
            {
                var result = existingDefinition.Compare(definition);
                result &= ~IndexDefinitionCompareDifferences.Etag;

                if (result == IndexDefinitionCompareDifferences.None)
                    return;

                result &= ~IndexDefinitionCompareDifferences.LockMode;
                result &= ~IndexDefinitionCompareDifferences.Priority;

                if (result != IndexDefinitionCompareDifferences.None)
                    throw new NotSupportedException($"Can not update auto-index: {definition.Name}");
            }

            AutoIndexes[definition.Name] = definition;
        }

        public void DeleteIndex(string name)
        {
            Indexes?.Remove(name);
            AutoIndexes?.Remove(name);
        }

        public void AddPeriodicBackupConfiguration(PeriodicBackupConfiguration configuration)
        {
            Debug.Assert(configuration.TaskId != 0);

            DeletePeriodicBackupConfiguration(configuration.TaskId);
            PeriodicBackups.Add(configuration);
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
    }

    public enum DeletionInProgressStatus
    {
        No,
        SoftDelete,
        HardDelete
    }
}
