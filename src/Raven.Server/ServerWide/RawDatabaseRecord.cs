﻿
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.ServerWide.Sharding;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.ServerWide
{
    public static class RawDatabaseRecordExtensions
    {
        public static DatabaseTopology TopologyForSubscriptions(this RawDatabaseRecord record)
        {
            if (record.IsSharded == false)
                return record.Topology;

            return record.Sharding.Orchestrator.Topology;
        }
    }

    public sealed class RawDatabaseRecord : IDisposable
    {
        private BlittableJsonReaderObject _record;
        private readonly JsonOperationContext _context;

        private DatabaseRecord _materializedRecord;

        public RawDatabaseRecord(JsonOperationContext context, BlittableJsonReaderObject record)
        {
            _context = context;
            _record = record ?? throw new ArgumentNullException(nameof(record));
        }

        private RawDatabaseRecord(DatabaseRecord record)
        {
            _materializedRecord = record;
        }

        public BlittableJsonReaderObject Raw
        {
            get
            {
                if (_record == null)
                    throw new ArgumentNullException(nameof(_record));

                return _record;
            }
        }

        private bool? _isDisabled;

        public bool IsDisabled
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Disabled;

                _isDisabled ??= _record.TryGet(nameof(DatabaseRecord.Disabled), out bool disabled) && disabled;

                return _isDisabled.Value;
            }
        }

        private bool? _isEncrypted;

        public bool IsEncrypted
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Encrypted;

                _isEncrypted ??= _record.TryGet(nameof(DatabaseRecord.Encrypted), out bool encrypted) && encrypted;

                return _isEncrypted.Value;
            }
        }

        private long? _etagForBackup;

        public long EtagForBackup
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.EtagForBackup;

                _etagForBackup ??= _record.TryGet(nameof(DatabaseRecord.EtagForBackup), out long etagForBackup)
                    ? etagForBackup
                    : 0;

                return _etagForBackup.Value;
            }
        }

        private string _databaseName;

        public string DatabaseName
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.DatabaseName;

                if (_databaseName == null)
                    _record.TryGet(nameof(DatabaseRecord.DatabaseName), out _databaseName);

                return _databaseName;
            }
        }

        public IEnumerable<(string Name, DatabaseTopology Topology)> Topologies
        {
            get
            {
                if (IsSharded)
                    return GetShardedDatabaseRecords().Select(x => (x.DatabaseName, x.Topology));

                return new[] { (DatabaseName, Topology) };
            }
        }

        private RawShardingConfiguration _sharding;

        public RawShardingConfiguration Sharding
        {
            get
            {
                if (_materializedRecord != null)
                {
                    if (_materializedRecord.Sharding == null)
                        return null;

                    _sharding = new RawShardingConfiguration(_materializedRecord.Sharding);

                    return _sharding;
                }

                if (_sharding == null)
                {
                    if (_record.TryGet(nameof(DatabaseRecord.Sharding), out BlittableJsonReaderObject obj) && obj != null)
                        _sharding = new RawShardingConfiguration(_context, obj);
                }

                return _sharding;
            }
        }

        private DatabaseTopology _topology;

        public DatabaseTopology Topology
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Topology;

                if (_topology == null && _record.TryGet(nameof(DatabaseRecord.Topology), out BlittableJsonReaderObject topologyJson) && topologyJson != null)
                {
                    _topology = JsonDeserializationCluster.DatabaseTopology(topologyJson);
                }

                return _topology;
            }
        }

        private bool? _isSharded;

        public bool IsSharded
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.IsSharded;

                if (_isSharded == null)
                {
                    var sharding = Sharding;
                    if (sharding != null)
                    {
                        sharding.Raw.TryGet(nameof(DatabaseRecord.Sharding.Shards), out BlittableJsonReaderObject shardTopologies);
                        _isSharded = shardTopologies is { Count: > 0 };
                    }
                    else
                    {
                        _isSharded = false;
                    }
                }

                return _isSharded.Value;
            }
        }

        private List<string> _supportedFeatures;

        public IReadOnlyList<string> SupportedFeatures
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.SupportedFeatures ?? new List<string>();

                if (_supportedFeatures == null && _record.TryGet(nameof(DatabaseRecord.SupportedFeatures), out BlittableJsonReaderArray supportedFeatures) && supportedFeatures != null)
                {
                    _supportedFeatures = new List<string>();
                    foreach (LazyStringValue supportedFeature in supportedFeatures)
                        _supportedFeatures.Add(supportedFeature);
                }

                return _supportedFeatures;
            }
        }

        public RawDatabaseRecord GetShardedDatabaseRecord(int shardNumber)
        {
            Sharding.Raw.TryGet(nameof(ShardingConfiguration.Shards), out BlittableJsonReaderObject shardTopologies);
            if (shardTopologies.TryGet(shardNumber.ToString(), out BlittableJsonReaderObject shardedTopology) == false)
                return null;
            
            var shardName = ShardHelper.ToShardName(DatabaseName, shardNumber);

            var settings = new Dictionary<string, string>();
            foreach (var setting in Settings)
                settings.Add(setting.Key, setting.Value);

            var dataDirectoryKey = RavenConfiguration.GetKey(x => x.Core.DataDirectory);
            if (settings.TryGetValue(dataDirectoryKey, out string dir))
                settings[dataDirectoryKey] = Path.Combine(dir, shardName);

            var sharding = Sharding.Raw;
            sharding.Modifications = new DynamicJsonValue(sharding)
            {
                [nameof(ShardingConfiguration.Shards)] = null
            };

            _record.Modifications = new DynamicJsonValue(_record)
            {
                [nameof(DatabaseRecord.DatabaseName)] = shardName,
                [nameof(DatabaseRecord.Topology)] = shardedTopology,
                [nameof(DatabaseRecord.Settings)] = DynamicJsonValue.Convert(settings)
            };

            return new RawDatabaseRecord(_context, _context.ReadObject(_record, shardName));
        }

        public IEnumerable<RawDatabaseRecord> AsShardsOrNormal(string nodeTag = null)
        {
            if (IsSharded == false)
            {
                if (nodeTag == null || Topology.RelevantFor(nodeTag))
                {
                    return new[] {this};
                }

                return Array.Empty<RawDatabaseRecord>();
            }

            return GetShardedDatabaseRecords(nodeTag);
        }

        public IEnumerable<RawDatabaseRecord> GetShardedDatabaseRecords(string nodeTag = null)
        {
            if (IsSharded == false)
                yield break;

            Sharding.Raw.TryGet(nameof(ShardingConfiguration.Shards), out BlittableJsonReaderObject shardTopologies);

            for (int i = 0; i < shardTopologies.Count; i++)
            {
                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                shardTopologies.GetPropertyByIndex(i, ref propertyDetails);

                if(propertyDetails.Value == null)
                    continue;

                var shardNumber = RawShardingConfiguration.GetShardNumberFromPropertyDetails(propertyDetails);

                var shardedDatabaseRecord = GetShardedDatabaseRecord(shardNumber);
                if (nodeTag == null || shardedDatabaseRecord.Topology.RelevantFor(nodeTag))
                    yield return shardedDatabaseRecord;
            }
        }

        public bool EntireDatabasePendingDeletion()
        {
            if (IsSharded)
            {
                if (DeletionInProgress.Count == 0)
                    return false;

                if (Sharding.Shards.Sum(x => x.Value.Count) == 0)
                    return true;

                int shard = 0;
                foreach (var shardTopology in Sharding.Shards)
                {
                    foreach (var nodeWithShard in shardTopology.Value.AllNodes)
                    {
                        if (DeletionInProgress.TryGetValue(DatabaseRecord.GetKeyForDeletionInProgress(nodeWithShard, shard), out var deletionStatus) == false || deletionStatus == DeletionInProgressStatus.No)
                        {
                            return false;
                        }
                    }

                    shard++;
                }
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal,
                    "Handle case where shard numbers are not consecutive");
                return true;
            }
            else
            {
                if (Topology.Count == 0)
                    return true;

                if (DeletionInProgress?.Count > 0)
                {
                    foreach (var node in Topology.AllNodes)
                    {
                        if (DeletionInProgress.TryGetValue(node, out var deletionStatus) == false || deletionStatus == DeletionInProgressStatus.No)
                            return false;
                    }

                    return true;
                }

                return false;
            }
        }

        public string GetClusterTransactionId()
        {
            if (_materializedRecord != null)
                return _materializedRecord.GetClusterTransactionId();

            if (IsSharded == false)
                return Topology.ClusterTransactionIdBase64;

            Debug.Assert(Sharding.Shards.All(s => s.Value.ClusterTransactionIdBase64.Equals(Sharding.Shards.ElementAt(0).Value.ClusterTransactionIdBase64)));
            return Sharding.Orchestrator.Topology.ClusterTransactionIdBase64;
        }

        private DatabaseStateStatus? _databaseState;

        public DatabaseStateStatus DatabaseState
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.DatabaseState;

                if (_databaseState == null && _record.TryGet(nameof(DatabaseRecord.DatabaseState), out _databaseState) == false)
                    _databaseState = DatabaseStateStatus.Normal;

                return _databaseState.Value;
            }
        }

        private DatabaseLockMode? _lockMode;

        public DatabaseLockMode LockMode
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.LockMode;

                if (_lockMode == null && _record.TryGet(nameof(DatabaseRecord.LockMode), out _lockMode) == false)
                    _lockMode = DatabaseLockMode.Unlock;

                return _lockMode.Value;
            }
        }

        private StudioConfiguration _studioConfiguration;

        public StudioConfiguration StudioConfiguration
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Studio;

                if (_studioConfiguration == null && _record.TryGet(nameof(DatabaseRecord.Studio), out BlittableJsonReaderObject config) && config != null)
                    _studioConfiguration = JsonDeserializationServer.StudioConfiguration(config);

                return _studioConfiguration;
            }
        }

        private TimeSeriesConfiguration _timeSeriesConfiguration;

        public TimeSeriesConfiguration TimeSeriesConfiguration
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.TimeSeries;

                if (_timeSeriesConfiguration == null && _record.TryGet(nameof(DatabaseRecord.TimeSeries), out BlittableJsonReaderObject config) && config != null)
                    _timeSeriesConfiguration = JsonDeserializationCluster.TimeSeriesConfiguration(config);

                return _timeSeriesConfiguration;
            }
        }

        private RevisionsConfiguration _revisionsConfiguration;

        public RevisionsConfiguration RevisionsConfiguration
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Revisions;

                if (_revisionsConfiguration == null && _record.TryGet(nameof(DatabaseRecord.Revisions), out BlittableJsonReaderObject config) && config != null)
                    _revisionsConfiguration = JsonDeserializationCluster.RevisionsConfiguration(config);

                return _revisionsConfiguration;
            }
        }

        private RevisionsCollectionConfiguration _revisionsForConflictsConfiguration;

        public RevisionsCollectionConfiguration RevisionsForConflicts
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.RevisionsForConflicts;

                if (_revisionsForConflictsConfiguration == null && _record.TryGet(nameof(DatabaseRecord.RevisionsForConflicts), out BlittableJsonReaderObject config) &&
                    config != null)
                    _revisionsForConflictsConfiguration = JsonDeserializationCluster.RevisionsCollectionConfiguration(config);

                return _revisionsForConflictsConfiguration;
            }
        }

        private DocumentsCompressionConfiguration _documentsCompressionConfiguration;

        public DocumentsCompressionConfiguration DocumentsCompressionConfiguration
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.DocumentsCompression;

                if (_documentsCompressionConfiguration == null && _record.TryGet(nameof(DatabaseRecord.DocumentsCompression), out BlittableJsonReaderObject config) && config != null)
                    _documentsCompressionConfiguration = JsonDeserializationCluster.DocumentsCompressionConfiguration(config);

                return _documentsCompressionConfiguration;
            }
        }

        private ConflictSolver _conflictSolverConfiguration;

        public ConflictSolver ConflictSolverConfiguration
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.ConflictSolverConfig;

                if (_conflictSolverConfiguration == null && _record.TryGet(nameof(DatabaseRecord.ConflictSolverConfig), out BlittableJsonReaderObject config) && config != null)
                    _conflictSolverConfiguration = JsonDeserializationCluster.ConflictSolverConfig(config);

                return _conflictSolverConfiguration;
            }
        }

        private ExpirationConfiguration _expirationConfiguration;

        public ExpirationConfiguration ExpirationConfiguration
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Expiration;

                if (_expirationConfiguration == null && _record.TryGet(nameof(DatabaseRecord.Expiration), out BlittableJsonReaderObject config) && config != null)
                    _expirationConfiguration = JsonDeserializationCluster.ExpirationConfiguration(config);

                return _expirationConfiguration;
            }
        }

        private RefreshConfiguration _refreshConfiguration;

        public RefreshConfiguration RefreshConfiguration
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Refresh;

                if (_refreshConfiguration == null && _record.TryGet(nameof(DatabaseRecord.Refresh), out BlittableJsonReaderObject config) && config != null)
                    _refreshConfiguration = JsonDeserializationCluster.RefreshConfiguration(config);

                return _refreshConfiguration;
            }
        }
        
        private DataArchivalConfiguration _dataArchivalConfiguration;
        
        public DataArchivalConfiguration DataArchivalConfiguration
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.DataArchival;

                if (_dataArchivalConfiguration == null && _record.TryGet(nameof(DatabaseRecord.DataArchival), out BlittableJsonReaderObject config) && config != null)
                    _dataArchivalConfiguration = JsonDeserializationCluster.DataArchivalConfiguration(config);

                return _dataArchivalConfiguration;
            }
        }

        private RetiredAttachmentsConfiguration _retiredAttachmentsConfiguration;

        public RetiredAttachmentsConfiguration RetiredAttachmentsConfiguration
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.RetiredAttachments;

                if (_retiredAttachmentsConfiguration == null && _record.TryGet(nameof(DatabaseRecord.RetiredAttachments), out BlittableJsonReaderObject config) && config != null)
                    _retiredAttachmentsConfiguration = JsonDeserializationCluster.RetiredAttachmentsConfiguration(config);

                return _retiredAttachmentsConfiguration;
            }
        }

        private List<ExternalReplication> _externalReplications;

        public List<ExternalReplication> ExternalReplications
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.ExternalReplications;

                if (_externalReplications != null) 
                    return _externalReplications;

                if (_record.TryGet(nameof(DatabaseRecord.ExternalReplications), out BlittableJsonReaderArray bjra) == false || bjra == null)
                    return new List<ExternalReplication>();
                
                _externalReplications = (from BlittableJsonReaderObject element in bjra
                    select JsonDeserializationClient.ExternalReplication(element)).ToList();
                
                return _externalReplications;
            }
        }

        private List<PullReplicationDefinition> _hubPullReplications;

        public List<PullReplicationDefinition> HubPullReplications
                {
            get
                    {
                if (_materializedRecord != null)
                    return _materializedRecord.HubPullReplications;

                if (_hubPullReplications != null)
                    return _hubPullReplications;
                
                if (_record.TryGet(nameof(DatabaseRecord.HubPullReplications), out BlittableJsonReaderArray bjra) == false || bjra == null)
                    return new List<PullReplicationDefinition>();

                _hubPullReplications = (from BlittableJsonReaderObject element in bjra
                    select JsonDeserializationClient.PullReplicationDefinition(element)).ToList();

                return _hubPullReplications;
                    }
                }

        private List<PullReplicationAsSink> _sinkPullReplications;

        public List<PullReplicationAsSink> SinkPullReplications
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.SinkPullReplications;

                if (_sinkPullReplications != null)
                    return _sinkPullReplications;

                if (_record.TryGet(nameof(DatabaseRecord.SinkPullReplications), out BlittableJsonReaderArray bjra) == false || bjra == null)
                    return new List<PullReplicationAsSink>();

                _sinkPullReplications = (from BlittableJsonReaderObject element in bjra
                    select JsonDeserializationClient.PullReplicationAsSink(element)).ToList();

                return _sinkPullReplications;
            }
        }

        public PullReplicationDefinition GetHubPullReplicationByName(string name)
        {
            if (_record.TryGet(nameof(DatabaseRecord.HubPullReplications), out BlittableJsonReaderArray bjra) && bjra != null)
            {
                foreach (BlittableJsonReaderObject element in bjra)
                {
                    if (element.TryGet(nameof(PullReplicationDefinition.Name), out string n) && n == name)
                        return JsonDeserializationClient.PullReplicationDefinition(element);
                }
            }

            return null;
        }

        public PullReplicationDefinition GetHubPullReplicationById(in long key)
        {
            if (_record.TryGet(nameof(DatabaseRecord.HubPullReplications), out BlittableJsonReaderArray bjra) && bjra != null)
            {
                foreach (BlittableJsonReaderObject element in bjra)
                {
                    if (element.TryGet(nameof(PullReplicationDefinition.TaskId), out long id) && id == key)
                        return JsonDeserializationClient.PullReplicationDefinition(element);
                }
            }

            return null;
        }

        public bool HubPullReplicationDefinitionExist()
        {
            return _record.TryGet(nameof(DatabaseRecord.HubPullReplications), out BlittableJsonReaderArray bjra) && (bjra?.Length > 0);
        }

        private List<long> _periodicBackupsTaskIds;

        public List<long> PeriodicBackupsTaskIds
        {
            get
            {
                if (_periodicBackupsTaskIds == null)
                {
                    if (_materializedRecord != null)
                    {
                        _periodicBackupsTaskIds = _materializedRecord
                            .PeriodicBackups
                            .Select(x => x.TaskId)
                            .ToList();
                    }
                    else
                    {
                        _periodicBackupsTaskIds = new List<long>();
                        if (_record.TryGet(nameof(DatabaseRecord.PeriodicBackups), out BlittableJsonReaderArray bjra) && bjra != null)
                        {
                            foreach (BlittableJsonReaderObject element in bjra)
                            {
                                if (element.TryGet(nameof(PeriodicBackupConfiguration.TaskId), out long taskId) == false)
                                    continue;

                                _periodicBackupsTaskIds.Add(taskId);
                            }
                        }
                    }
                }

                return _periodicBackupsTaskIds;
            }
        }

        private List<PeriodicBackupConfiguration> _periodicBackups;

        public List<PeriodicBackupConfiguration> PeriodicBackups
        {
            get
            {
                if (_periodicBackups != null) 
                    return _periodicBackups;

                if (_materializedRecord != null)
                {
                    _periodicBackups = _materializedRecord.PeriodicBackups;
                }
                else
                {
                    _periodicBackups = new List<PeriodicBackupConfiguration>();
                    if (_record.TryGet(nameof(DatabaseRecord.PeriodicBackups), out BlittableJsonReaderArray periodicBackups) && periodicBackups != null)
                    {
                        foreach (BlittableJsonReaderObject backup in periodicBackups)
                        {
                            var backupConfiguration = JsonDeserializationServer.GetPeriodicBackupConfiguration(backup);
                            _periodicBackups.Add(backupConfiguration);
                        }
                    }
                }

                return _periodicBackups;
            }
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

        private List<RavenEtlConfiguration> _ravenEtls;

        public List<RavenEtlConfiguration> RavenEtls
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.RavenEtls;

                if (_ravenEtls == null)
                {
                    _ravenEtls = new List<RavenEtlConfiguration>();
                    if (_record.TryGet(nameof(DatabaseRecord.RavenEtls), out BlittableJsonReaderArray bjra) && bjra != null)
                    {
                        foreach (BlittableJsonReaderObject element in bjra)
                            _ravenEtls.Add(JsonDeserializationCluster.RavenEtlConfiguration(element));
                    }
                }

                return _ravenEtls;
            }
        }

        private List<SqlEtlConfiguration> _sqlEtls;

        public List<SqlEtlConfiguration> SqlEtls
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.SqlEtls;

                if (_sqlEtls == null)
                {
                    _sqlEtls = new List<SqlEtlConfiguration>();
                    if (_record.TryGet(nameof(DatabaseRecord.SqlEtls), out BlittableJsonReaderArray bjra) && bjra != null)
                    {
                        foreach (BlittableJsonReaderObject element in bjra)
                            _sqlEtls.Add(JsonDeserializationCluster.SqlEtlConfiguration(element));
                    }
                }

                return _sqlEtls;
            }
        }


        private List<OlapEtlConfiguration> _olapEtls;

        public List<OlapEtlConfiguration> OlapEtls
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.OlapEtls;

                if (_olapEtls == null)
                {
                    _olapEtls = new List<OlapEtlConfiguration>();
                    if (_record.TryGet(nameof(DatabaseRecord.OlapEtls), out BlittableJsonReaderArray bjra) && bjra != null)
                    {
                        foreach (BlittableJsonReaderObject element in bjra)
                            _olapEtls.Add(JsonDeserializationCluster.OlapEtlConfiguration(element));
                    }
                }

                return _olapEtls;
            }
        }

        private List<ElasticSearchEtlConfiguration> _elasticSearchEtls;

        public List<ElasticSearchEtlConfiguration> ElasticSearchEtls
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.ElasticSearchEtls;

                if (_elasticSearchEtls == null)
                {
                    _elasticSearchEtls = new List<ElasticSearchEtlConfiguration>();
                    if (_record.TryGet(nameof(DatabaseRecord.ElasticSearchEtls), out BlittableJsonReaderArray bjra) && bjra != null)
                    {
                        foreach (BlittableJsonReaderObject element in bjra)
                            _elasticSearchEtls.Add(JsonDeserializationCluster.ElasticSearchEtlConfiguration(element));
                    }
                }

                return _elasticSearchEtls;
            }
        }

        private List<QueueEtlConfiguration> _queueEtls;

        public List<QueueEtlConfiguration> QueueEtls
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.QueueEtls;

                if (_queueEtls == null)
                {
                    _queueEtls = new List<QueueEtlConfiguration>();
                    if (_record.TryGet(nameof(DatabaseRecord.QueueEtls), out BlittableJsonReaderArray bjra) && bjra != null)
                    {
                        foreach (BlittableJsonReaderObject element in bjra)
                            _queueEtls.Add(JsonDeserializationCluster.QueueEtlConfiguration(element));
                    }
                }

                return _queueEtls;
            }
        }

        private List<QueueSinkConfiguration> _queueSinks;

        public List<QueueSinkConfiguration> QueueSinks
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.QueueSinks;

                if (_queueSinks == null)
                {
                    _queueSinks = new List<QueueSinkConfiguration>();
                    if (_record.TryGet(nameof(DatabaseRecord.QueueSinks), out BlittableJsonReaderArray bjra) && bjra != null)
                    {
                        foreach (BlittableJsonReaderObject element in bjra)
                            _queueSinks.Add(JsonDeserializationCluster.QueueSinkConfiguration(element));
                    }
                }

                return _queueSinks;
            }
        }

        private Dictionary<string, string> _settings;

        public Dictionary<string, string> Settings
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Settings;

                if (_settings == null)
                {
                    _settings = new Dictionary<string, string>();
                    if (_record.TryGet(nameof(DatabaseRecord.Settings), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            _settings[propertyDetails.Name] = propertyDetails.Value.ToString();
                        }
                    }
                }

                return _settings;
            }
        }

        private Dictionary<string, DeletionInProgressStatus> _deletionInProgress;

        public Dictionary<string, DeletionInProgressStatus> DeletionInProgress
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.DeletionInProgress;

                if (_deletionInProgress == null)
                {
                    _deletionInProgress = new Dictionary<string, DeletionInProgressStatus>();
                    if (_record.TryGet(nameof(DatabaseRecord.DeletionInProgress), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (Enum.TryParse(propertyDetails.Value.ToString(), out DeletionInProgressStatus result))
                                _deletionInProgress[propertyDetails.Name] = result;
                        }
                    }
                }

                return _deletionInProgress;
            }
        }


        private Dictionary<string, RollingIndex> _rollingIndexes;

        public Dictionary<string, RollingIndex> RollingIndexes
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.RollingIndexes;

                if (_rollingIndexes == null)
                {
                    if (_record.TryGet(nameof(DatabaseRecord.RollingIndexes), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        _rollingIndexes = new Dictionary<string, RollingIndex>();
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                                _rollingIndexes[propertyDetails.Name] = JsonDeserializationCluster.RollingIndexes(bjro);
                        }
                    }
                }
                return _rollingIndexes;
            }
        }

        private Dictionary<string, List<IndexHistoryEntry>> _indexesHistory;

        public Dictionary<string, List<IndexHistoryEntry>> IndexesHistory
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.IndexesHistory;

                if (_indexesHistory == null)
                {
                    _indexesHistory = new Dictionary<string, List<IndexHistoryEntry>>();
                    if (_record.TryGet(nameof(DatabaseRecord.IndexesHistory), out BlittableJsonReaderObject obj) && obj != null)
                    {
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
                                    list.Add(JsonDeserializationCluster.IndexHistoryEntry(element));

                                _indexesHistory[propertyDetails.Name] = list;
                            }
                        }
                    }
                }

                return _indexesHistory;
            }
        }

        private int? _countOfAutoIndexes;

        public int CountOfAutoIndexes
        {
            get
            {
                if (_materializedRecord != null)
                    return (_materializedRecord.AutoIndexes?.Count ?? 0);

                if (_countOfAutoIndexes == null)
                {
                    _countOfAutoIndexes = 0;
                    if (_record.TryGet(nameof(DatabaseRecord.AutoIndexes), out BlittableJsonReaderObject  obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject)
                                _countOfAutoIndexes++;
                        }
                    }
                }

                return _countOfAutoIndexes.Value;
            }
        }

        private int? _countOfStaticIndexes;

        public int CountOfStaticIndexes
        {
            get
            {
                if (_materializedRecord != null)
                    return (_materializedRecord.Indexes?.Count ?? 0);

                if (_countOfStaticIndexes == null)
                {
                    _countOfStaticIndexes = 0;
                    if (_record.TryGet(nameof(DatabaseRecord.Indexes), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject)
                                _countOfStaticIndexes++;
                        }
                    }
                }

                return _countOfStaticIndexes.Value;
            }
        }

        public int CountOfIndexes => CountOfStaticIndexes + CountOfAutoIndexes;

        private Dictionary<string, IndexDefinition> _indexes;

        public Dictionary<string, IndexDefinition> Indexes
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Indexes;

                if (_indexes == null)
                {
                    _indexes = new Dictionary<string, IndexDefinition>();
                    if (_record.TryGet(nameof(DatabaseRecord.Indexes), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                                _indexes[propertyDetails.Name] = JsonDeserializationCluster.IndexDefinition(bjro);
                        }
                    }
                }

                return _indexes;
            }
        }

        private Dictionary<string, AutoIndexDefinition> _autoIndexes;

        public Dictionary<string, AutoIndexDefinition> AutoIndexes
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.AutoIndexes;

                if (_autoIndexes == null)
                {
                    _autoIndexes = new Dictionary<string, AutoIndexDefinition>();
                    if (_record.TryGet(nameof(DatabaseRecord.AutoIndexes), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                                _autoIndexes[propertyDetails.Name] = JsonDeserializationCluster.AutoIndexDefinition(bjro);
                        }
                    }
                }

                return _autoIndexes;
            }
        }

        private Dictionary<string, SorterDefinition> _sorters;

        public Dictionary<string, SorterDefinition> Sorters
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Sorters;

                if (_sorters == null)
                {
                    _sorters = new Dictionary<string, SorterDefinition>();
                    if (_record.TryGet(nameof(DatabaseRecord.Sorters), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                                _sorters[propertyDetails.Name] = JsonDeserializationServer.SorterDefinition(bjro);
                        }
                    }
                }

                return _sorters;
            }
        }

        private int? _countOfSorters;

        public int CountOfSorters
        {
            get
            {
                if (_materializedRecord != null)
                    return (_materializedRecord.Sorters?.Count ?? 0);

                if (_countOfSorters == null)
                {
                    _countOfSorters = 0;
                    if (_record.TryGet(nameof(DatabaseRecord.Sorters), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject)
                                _countOfSorters++;
                        }
                    }
                }

                return _countOfSorters.Value;
            }
        }

        private Dictionary<string, AnalyzerDefinition> _analyzers;

        public Dictionary<string, AnalyzerDefinition> Analyzers
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.Analyzers;

                if (_analyzers == null)
                {
                    _analyzers = new Dictionary<string, AnalyzerDefinition>();
                    if (_record.TryGet(nameof(DatabaseRecord.Analyzers), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                                _analyzers[propertyDetails.Name] = JsonDeserializationServer.AnalyzerDefinition(bjro);
                        }
                    }
                }

                return _analyzers;
            }
        }

        private int? _countOfAnalyzers;

        public int CountOfAnalyzers
        {
            get
            {
                if (_materializedRecord != null)
                    return (_materializedRecord.Analyzers?.Count ?? 0);

                if (_countOfAnalyzers == null)
                {
                    _countOfAnalyzers = 0;
                    if (_record.TryGet(nameof(DatabaseRecord.Analyzers), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject)
                                _countOfAnalyzers++;
                        }
                    }
                }

                return _countOfAnalyzers.Value;
            }
        }

        private Dictionary<string, SqlConnectionString> _sqlConnectionStrings;

        public Dictionary<string, SqlConnectionString> SqlConnectionStrings
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.SqlConnectionStrings;

                if (_sqlConnectionStrings == null)
                {
                    _sqlConnectionStrings = new Dictionary<string, SqlConnectionString>();
                    if (_record.TryGet(nameof(DatabaseRecord.SqlConnectionStrings), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                                _sqlConnectionStrings[propertyDetails.Name] = JsonDeserializationCluster.SqlConnectionString(bjro);
                        }
                    }
                }

                return _sqlConnectionStrings;
            }
        }

        private Dictionary<string, ElasticSearchConnectionString> _elasticSearchConnectionStrings;

        public Dictionary<string, ElasticSearchConnectionString> ElasticSearchConnectionStrings
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.ElasticSearchConnectionStrings;

                if (_elasticSearchConnectionStrings == null)
                {
                    _elasticSearchConnectionStrings = new Dictionary<string, ElasticSearchConnectionString>();
                    if (_record.TryGet(nameof(DatabaseRecord.ElasticSearchConnectionStrings), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                                _elasticSearchConnectionStrings[propertyDetails.Name] = JsonDeserializationCluster.ElasticSearchConnectionString(bjro);
                        }
                    }
                }

                return _elasticSearchConnectionStrings;
            }
        }

        private Dictionary<string, RavenConnectionString> _ravenConnectionStrings;

        public Dictionary<string, RavenConnectionString> RavenConnectionStrings
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.RavenConnectionStrings;

                if (_ravenConnectionStrings == null)
                {
                    _ravenConnectionStrings = new Dictionary<string, RavenConnectionString>();
                    if (_record.TryGet(nameof(DatabaseRecord.RavenConnectionStrings), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                                _ravenConnectionStrings[propertyDetails.Name] = JsonDeserializationCluster.RavenConnectionString(bjro);
                        }
                    }
                }

                return _ravenConnectionStrings;
            }
        }

        private Dictionary<string, OlapConnectionString> _olapConnectionStrings;

        public Dictionary<string, OlapConnectionString> OlapConnectionString
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.OlapConnectionStrings;

                if (_olapConnectionStrings == null)
                {
                    _olapConnectionStrings = new Dictionary<string, OlapConnectionString>();
                    if (_record.TryGet(nameof(DatabaseRecord.OlapConnectionStrings), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                                _olapConnectionStrings[propertyDetails.Name] = JsonDeserializationCluster.OlapConnectionString(bjro);
                        }
                    }
                }

                return _olapConnectionStrings;
            }
        }

        private Dictionary<string, QueueConnectionString> _queueConnectionStrings;

        public Dictionary<string, QueueConnectionString> QueueConnectionStrings
        {
            get
            {
                if (_materializedRecord != null)
                    return _materializedRecord.QueueConnectionStrings;

                if (_queueConnectionStrings == null)
                {
                    _queueConnectionStrings = new Dictionary<string, QueueConnectionString>();
                    if (_record.TryGet(nameof(DatabaseRecord.QueueConnectionStrings), out BlittableJsonReaderObject obj) && obj != null)
                    {
                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < obj.Count; i++)
                        {
                            obj.GetPropertyByIndex(i, ref propertyDetails);

                            if (propertyDetails.Value == null)
                                continue;

                            if (propertyDetails.Value is BlittableJsonReaderObject bjro)
                                _queueConnectionStrings[propertyDetails.Name] = JsonDeserializationCluster.QueueConnectionString(bjro);
                        }
                    }
                }

                return _queueConnectionStrings;
            }
        }

        internal bool IsShardBeingDeletedOnAnyNode(int shardNumber)
        {
            foreach (var deletion in DeletionInProgress)
            {
                if (deletion.Key.Contains($"${shardNumber}"))
                    return true;
            }

            return false;
        }

        public void Dispose()
        {
            _record?.Dispose();
            _record = null;
        }

        public DatabaseRecord MaterializedRecord
        {
            get
            {
                if (_materializedRecord == null)
                {
                    _materializedRecord = JsonDeserializationCluster.DatabaseRecord(_record);
                    Dispose();
                }

                return _materializedRecord;
            }
        }

        public static implicit operator DatabaseRecord(RawDatabaseRecord raw) => raw.MaterializedRecord;
        public static implicit operator RawDatabaseRecord(DatabaseRecord record) => new RawDatabaseRecord(record);
    }
}
