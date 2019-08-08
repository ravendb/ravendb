using System;
using System.Collections.Generic;
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
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide
{
    public class RawDatabaseRecord : IDisposable
    {
        private readonly JsonOperationContext _context;
        private readonly BlittableJsonReaderObject _record;

        public RawDatabaseRecord(JsonOperationContext context, BlittableJsonReaderObject record)
        {
            _context = context;
            _record = record;
        }

        public BlittableJsonReaderObject GetRecord()
        {
            return _record;
        }

        public bool IsDisabled()
        {
            if (_record.TryGet(nameof(DatabaseRecord.Disabled), out bool disabled) == false)
                return false;

            return disabled;
        }

        public bool IsEncrypted()
        {
            if (_record.TryGet(nameof(DatabaseRecord.Encrypted), out bool encrypted) == false)
                return false;

            return encrypted;
        }

        public long GetEtagForBackup()
        {
            if (_record.TryGet(nameof(DatabaseRecord.EtagForBackup), out long etagForBackup) == false)
                return 0;

            return etagForBackup;
        }

        public string GetDatabaseName()
        {
            _record.TryGet(nameof(DatabaseRecord.DatabaseName), out string databaseName);
            return databaseName;
        }

        public DatabaseTopology GetTopology()
        {
            if (_record.TryGet(nameof(DatabaseRecord.Topology), out BlittableJsonReaderObject rawTopology) == false 
                || rawTopology == null)
                return null;

            return JsonDeserializationCluster.DatabaseTopology(rawTopology);
        }

        public bool IsSharded()
        {
            _record.TryGet(nameof(DatabaseRecord.Shards), out BlittableJsonReaderArray array);
            return array != null && array.Length > 0;
        }

        public RawDatabaseRecord GetShardedDatabaseRecord(int index)
        {
            _record.TryGet(nameof(DatabaseRecord.Shards), out BlittableJsonReaderArray array);
            var name = GetDatabaseName();
            var shardedTopology = (BlittableJsonReaderObject)array[index];
            var shardName = name + "$" + index;
            _record.Modifications = new DynamicJsonValue(_record)
            {
                [nameof(DatabaseRecord.DatabaseName)] = shardName,
                [nameof(DatabaseRecord.Topology)] = shardedTopology,
                [nameof(DatabaseRecord.ShardAllocations)] = null,
                [nameof(DatabaseRecord.Shards)] = null,
            };

            return new RawDatabaseRecord(_context, _context.ReadObject(_record, shardName));
        }

        public IEnumerable<RawDatabaseRecord> GetShardedDatabaseRecords()
        {
            if(_record.TryGet(nameof(DatabaseRecord.Shards), out BlittableJsonReaderArray array) == false 
               || array == null)
                yield break;

            var name = GetDatabaseName();

            for (var index = 0; index < array.Length; index++)
            {
                var shardedTopology = (BlittableJsonReaderObject)array[index];
                var shardName = name + "$" + index;
                _record.Modifications = new DynamicJsonValue(_record)
                {
                    [nameof(DatabaseRecord.DatabaseName)] = shardName,
                    [nameof(DatabaseRecord.Topology)] = shardedTopology,
                    [nameof(DatabaseRecord.ShardAllocations)] = null,
                    [nameof(DatabaseRecord.Shards)] = null,
                };

                yield return new RawDatabaseRecord(_context, _context.ReadObject(_record, shardName));
            }
        }

        public DatabaseStateStatus GetDatabaseStateStatus()
        {
            if (_record.TryGet(nameof(DatabaseRecord.DatabaseState), out DatabaseStateStatus rawDatabaseStateStatus) == false)
            {
                return DatabaseStateStatus.Normal;
            }

            return rawDatabaseStateStatus;
        }

        public RevisionsConfiguration GetRevisionsConfiguration()
        {
            if (_record.TryGet(nameof(DatabaseRecord.Revisions), out BlittableJsonReaderObject config) == false || config == null)
            {
                return null;
            }

            return JsonDeserializationCluster.RevisionsConfiguration(config);
        }

        public ConflictSolver GetConflictSolverConfiguration()
        {
            if (_record.TryGet(nameof(DatabaseRecord.ConflictSolverConfig), out BlittableJsonReaderObject config) == false || config == null)
            {
                return null;
            }

            return JsonDeserializationCluster.ConflictSolverConfig(config);
        }

        public ExpirationConfiguration GetExpirationConfiguration()
        {
            if (_record.TryGet(nameof(DatabaseRecord.Expiration), out BlittableJsonReaderObject config) == false || config == null)
            {
                return null;
            }

            return JsonDeserializationCluster.ExpirationConfiguration(config);
        }
        
        public RefreshConfiguration GetRefreshConfiguration()
        {
            if (_record.TryGet(nameof(DatabaseRecord.Refresh), out BlittableJsonReaderObject config) == false || config == null)
            {
                return null;
            }

            return JsonDeserializationCluster.RefreshConfiguration(config);
        }

        public List<ExternalReplication> GetExternalReplications()
        {
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

        public Dictionary<string, IndexDefinition> GetIndexes()
        {
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
                {
                    dictionary[propertyDetails.Name] = JsonDeserializationCluster.IndexDefinition(bjro);
                }
            }

            return dictionary;
        }

        public Dictionary<string, AutoIndexDefinition> GetAutoIndexes()
        {
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
        }
    }
}
