using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.ServerWide;
using Sparrow.Json;

namespace Raven.Server.ServerWide
{
    public class RawDatabaseRecord : IDisposable
    {
        private BlittableJsonReaderObject _record;

        private DatabaseRecord _materializedRecord;

        public RawDatabaseRecord(BlittableJsonReaderObject record)
        {
            _record = record ?? throw new ArgumentNullException(nameof(record));
        }

        public BlittableJsonReaderObject GetRecord()
        {
            return _record;
        }

        public bool IsDisabled()
        {
            if (_materializedRecord != null)
                return _materializedRecord.Disabled;

            if (_record.TryGet(nameof(DatabaseRecord.Disabled), out bool disabled) == false)
                return false;

            return disabled;
        }

        public bool IsEncrypted()
        {
            if (_materializedRecord != null)
                return _materializedRecord.Encrypted;

            if (_record.TryGet(nameof(DatabaseRecord.Encrypted), out bool encrypted) == false)
                return false;

            return encrypted;
        }

        public long GetEtagForBackup()
        {
            if (_materializedRecord != null)
                return _materializedRecord.EtagForBackup;

            if (_record.TryGet(nameof(DatabaseRecord.EtagForBackup), out long etagForBackup) == false)
                return 0;

            return etagForBackup;
        }

        public string GetDatabaseName()
        {
            if (_materializedRecord != null)
                return _materializedRecord.DatabaseName;

            _record.TryGet(nameof(DatabaseRecord.DatabaseName), out string databaseName);
            return databaseName;
        }

        public DatabaseTopology GetTopology()
        {
            if (_materializedRecord != null)
                return _materializedRecord.Topology;

            if (_record.TryGet(nameof(DatabaseRecord.Topology), out BlittableJsonReaderObject rawTopology) == false)
                return null;

            return JsonDeserializationCluster.DatabaseTopology(rawTopology);
        }

        public DatabaseStateStatus GetDatabaseStateStatus()
        {
            if (_materializedRecord != null)
                return _materializedRecord.DatabaseState;

            if (_record.TryGet(nameof(DatabaseRecord.DatabaseState), out DatabaseStateStatus rawDatabaseStateStatus) == false)
            {
                return DatabaseStateStatus.Normal;
            }

            return rawDatabaseStateStatus;
        }

        public RevisionsConfiguration GetRevisionsConfiguration()
        {
            if (_materializedRecord != null)
                return _materializedRecord.Revisions;

            if (_record.TryGet(nameof(DatabaseRecord.Revisions), out BlittableJsonReaderObject config) == false || config == null)
            {
                return null;
            }

            return JsonDeserializationCluster.RevisionsConfiguration(config);
        }

        public ConflictSolver GetConflictSolverConfiguration()
        {
            if (_materializedRecord != null)
                return _materializedRecord.ConflictSolverConfig;

            if (_record.TryGet(nameof(DatabaseRecord.ConflictSolverConfig), out BlittableJsonReaderObject config) == false || config == null)
            {
                return null;
            }

            return JsonDeserializationCluster.ConflictSolverConfig(config);
        }

        public ExpirationConfiguration GetExpirationConfiguration()
        {
            if (_materializedRecord != null)
                return _materializedRecord.Expiration;

            if (_record.TryGet(nameof(DatabaseRecord.Expiration), out BlittableJsonReaderObject config) == false || config == null)
            {
                return null;
            }

            return JsonDeserializationCluster.ExpirationConfiguration(config);
        }

        public RefreshConfiguration GetRefreshConfiguration()
        {
            if (_materializedRecord != null)
                return _materializedRecord.Refresh;

            if (_record.TryGet(nameof(DatabaseRecord.Refresh), out BlittableJsonReaderObject config) == false || config == null)
            {
                return null;
            }

            return JsonDeserializationCluster.RefreshConfiguration(config);
        }

        public List<ExternalReplication> GetExternalReplications()
        {
            if (_materializedRecord != null)
                return _materializedRecord.ExternalReplications;

            if (_record.TryGet(nameof(DatabaseRecord.ExternalReplications), out BlittableJsonReaderArray bjra) == false || bjra == null)
            {
                return null;
            }

            var list = new List<ExternalReplication>();

            foreach (BlittableJsonReaderObject element in bjra)
            {
                list.Add(JsonDeserializationCluster.ExternalReplication(element));
            }

            return list;
        }

        public List<PullReplicationDefinition> GetHubPullReplications()
        {
            if (_materializedRecord != null)
                return _materializedRecord.HubPullReplications;

            if (_record.TryGet(nameof(DatabaseRecord.HubPullReplications), out BlittableJsonReaderArray bjra) == false || bjra == null)
            {
                return null;
            }

            var list = new List<PullReplicationDefinition>();

            foreach (BlittableJsonReaderObject element in bjra)
            {
                list.Add(JsonDeserializationCluster.PullReplicationDefinition(element));
            }

            return list;
        }

        public List<long> GetPeriodicBackupsTaskIds()
        {
            if (_materializedRecord != null)
            {
                return _materializedRecord
                    .PeriodicBackups
                    .Select(x => x.TaskId)
                    .ToList();
            }

            if (_record.TryGet(nameof(DatabaseRecord.PeriodicBackups), out BlittableJsonReaderArray bjra) == false || bjra == null)
                return null;

            var list = new List<long>();

            foreach (BlittableJsonReaderObject element in bjra)
            {
                if (element.TryGet(nameof(PeriodicBackupConfiguration.TaskId), out long taskId) == false)
                    continue;

                list.Add(taskId);
            }

            return list;
        }

        public PeriodicBackupConfiguration GetPeriodicBackupConfiguration(long taskId)
        {
            if (_materializedRecord != null)
                return _materializedRecord.PeriodicBackups.Find(x => x.TaskId == taskId);

            if (_record.TryGet(nameof(DatabaseRecord.PeriodicBackups), out BlittableJsonReaderArray bjra) == false || bjra == null)
                return null;

            foreach (BlittableJsonReaderObject element in bjra)
            {
                if (element.TryGet(nameof(PeriodicBackupConfiguration.TaskId), out long configurationTaskId) == false)
                    continue;

                if (taskId == configurationTaskId)
                    return JsonDeserializationCluster.PeriodicBackupConfiguration(element);
            }

            return null;
        }

        public List<RavenEtlConfiguration> GetRavenEtls()
        {
            if (_materializedRecord != null)
                return _materializedRecord.RavenEtls;

            if (_record.TryGet(nameof(DatabaseRecord.RavenEtls), out BlittableJsonReaderArray bjra) == false || bjra == null)
            {
                return null;
            }

            var list = new List<RavenEtlConfiguration>();

            foreach (BlittableJsonReaderObject element in bjra)
            {
                list.Add(JsonDeserializationCluster.RavenEtlConfiguration(element));
            }

            return list;
        }

        public List<SqlEtlConfiguration> GetSqlEtls()
        {
            if (_materializedRecord != null)
                return _materializedRecord.SqlEtls;

            if (_record.TryGet(nameof(DatabaseRecord.SqlEtls), out BlittableJsonReaderArray bjra) == false || bjra == null)
            {
                return null;
            }

            var list = new List<SqlEtlConfiguration>();

            foreach (BlittableJsonReaderObject element in bjra)
            {
                list.Add(JsonDeserializationCluster.SqlEtlConfiguration(element));
            }

            return list;
        }

        public Dictionary<string, string> GetSettings()
        {
            if (_materializedRecord != null)
                return _materializedRecord.Settings;

            if (_record.TryGet(nameof(DatabaseRecord.Settings), out BlittableJsonReaderObject obj) == false || obj == null)
                return null;

            var dictionary = new Dictionary<string, string>();

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < obj.Count; i++)
            {
                obj.GetPropertyByIndex(i, ref propertyDetails);

                if (propertyDetails.Value == null)
                    continue;

                if (propertyDetails.Value is string str)
                {
                    dictionary[propertyDetails.Name] = str;
                }
            }

            return dictionary;
        }

        public Dictionary<string, DeletionInProgressStatus> GetDeletionInProgressStatus()
        {
            if (_materializedRecord != null)
                return _materializedRecord.DeletionInProgress;

            if (_record.TryGet(nameof(DatabaseRecord.DeletionInProgress), out BlittableJsonReaderObject obj) == false || obj == null)
                return null;

            var dictionary = new Dictionary<string, DeletionInProgressStatus>();

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < obj.Count; i++)
            {
                obj.GetPropertyByIndex(i, ref propertyDetails);

                if (propertyDetails.Value == null)
                    continue;

                if (Enum.TryParse(propertyDetails.Value.ToString(), out DeletionInProgressStatus result))
                    dictionary[propertyDetails.Name] = result;
            }

            return dictionary;
        }

        public Dictionary<string, List<IndexHistoryEntry>> GetIndexesHistory()
        {
            if (_materializedRecord != null)
                return _materializedRecord.IndexesHistory;

            if (_record.TryGet(nameof(DatabaseRecord.IndexesHistory), out BlittableJsonReaderObject obj) == false || obj == null)
            {
                return null;
            }

            var dictionary = new Dictionary<string, List<IndexHistoryEntry>>();

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < obj.Count; i++)
            {
                obj.GetPropertyByIndex(i, ref propertyDetails);

                if (propertyDetails.Value == null)
                    continue;

                if (propertyDetails.Value is BlittableJsonReaderArray bjra)
                {
                    var list = new List<IndexHistoryEntry>();
                    foreach (BlittableJsonReaderObject element in bjra)
                    {
                        list.Add(JsonDeserializationCluster.IndexHistoryEntry(element));
                    }
                    dictionary[propertyDetails.Name] = list;
                }
            }

            return dictionary;
        }

        public int GetIndexesCount()
        {
            if (_materializedRecord != null)
                return _materializedRecord.Indexes?.Count ?? 0;

            if (_record.TryGet(nameof(DatabaseRecord.Indexes), out BlittableJsonReaderObject obj) == false || obj == null)
            {
                return 0;
            }

            var count = 0;
            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < obj.Count; i++)
            {
                obj.GetPropertyByIndex(i, ref propertyDetails);

                if (propertyDetails.Value == null)
                    continue;

                if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                    count++;
            }

            return count;
        }

        public Dictionary<string, IndexDefinition> GetIndexes()
        {
            if (_materializedRecord != null)
                return _materializedRecord.Indexes;

            if (_record.TryGet(nameof(DatabaseRecord.Indexes), out BlittableJsonReaderObject obj) == false || obj == null)
            {
                return null;
            }

            var dictionary = new Dictionary<string, IndexDefinition>();

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < obj.Count; i++)
            {
                obj.GetPropertyByIndex(i, ref propertyDetails);

                if (propertyDetails.Value == null)
                    continue;

                if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                    dictionary[propertyDetails.Name] = JsonDeserializationCluster.IndexDefinition(bjro);
            }

            return dictionary;
        }

        public Dictionary<string, AutoIndexDefinition> GetAutoIndexes()
        {
            if (_materializedRecord != null)
                return _materializedRecord.AutoIndexes;

            if (_record.TryGet(nameof(DatabaseRecord.AutoIndexes), out BlittableJsonReaderObject obj) == false || obj == null)
            {
                return null;
            }

            var dictionary = new Dictionary<string, AutoIndexDefinition>();

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < obj.Count; i++)
            {
                obj.GetPropertyByIndex(i, ref propertyDetails);

                if (propertyDetails.Value == null)
                    continue;

                if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                {
                    dictionary[propertyDetails.Name] = JsonDeserializationCluster.AutoIndexDefinition(bjro);
                }
            }

            return dictionary;
        }

        public Dictionary<string, SorterDefinition> GetSorters()
        {
            if (_materializedRecord != null)
                return _materializedRecord.Sorters;

            if (_record.TryGet(nameof(DatabaseRecord.Sorters), out BlittableJsonReaderObject obj) == false || obj == null)
            {
                return null;
            }

            var dictionary = new Dictionary<string, SorterDefinition>();

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < obj.Count; i++)
            {
                obj.GetPropertyByIndex(i, ref propertyDetails);

                if (propertyDetails.Value == null)
                    continue;

                if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                {
                    dictionary[propertyDetails.Name] = JsonDeserializationCluster.SorterDefinition(bjro);
                }
            }

            return dictionary;
        }

        public Dictionary<string, SqlConnectionString> GetSqlConnectionStrings()
        {
            if (_materializedRecord != null)
                return _materializedRecord.SqlConnectionStrings;

            if (_record.TryGet(nameof(DatabaseRecord.SqlConnectionStrings), out BlittableJsonReaderObject obj) == false || obj == null)
            {
                return null;
            }

            var dictionary = new Dictionary<string, SqlConnectionString>();

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < obj.Count; i++)
            {
                obj.GetPropertyByIndex(i, ref propertyDetails);

                if (propertyDetails.Value == null)
                    continue;

                if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                {
                    dictionary[propertyDetails.Name] = JsonDeserializationCluster.SqlConnectionString(bjro);
                }
            }

            return dictionary;
        }

        public Dictionary<string, RavenConnectionString> GetRavenConnectionStrings()
        {
            if (_materializedRecord != null)
                return _materializedRecord.RavenConnectionStrings;

            if (_record.TryGet(nameof(DatabaseRecord.RavenConnectionStrings), out BlittableJsonReaderObject obj) == false || obj == null)
            {
                return null;
            }

            var dictionary = new Dictionary<string, RavenConnectionString>();

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < obj.Count; i++)
            {
                obj.GetPropertyByIndex(i, ref propertyDetails);

                if (propertyDetails.Value == null)
                    continue;

                if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                {
                    dictionary[propertyDetails.Name] = JsonDeserializationCluster.RavenConnectionString(bjro);
                }
            }

            return dictionary;
        }

        public void Dispose()
        {
            _record?.Dispose();
            _record = null;
        }

        public DatabaseRecord GetMaterializedRecord()
        {
            if (_materializedRecord == null)
            {
                _materializedRecord = JsonDeserializationCluster.DatabaseRecord(_record);
                Dispose();
            }

            return _materializedRecord;
        }
    }
}
