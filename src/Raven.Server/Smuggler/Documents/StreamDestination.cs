using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Counters;
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
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using BackupUtils = Raven.Server.Utils.BackupUtils;
using ShardingConfiguration = Raven.Client.ServerWide.Sharding.ShardingConfiguration;

namespace Raven.Server.Smuggler.Documents
{
    public sealed class StreamDestination : ISmugglerDestination
    {
        private readonly Stream _stream;
        private Stream _outputStream;
        private readonly JsonOperationContext _context;
        private readonly ISmugglerSource _source;
        private readonly CompressionLevel _compressionLevel;
        private readonly ExportCompressionAlgorithm _compressionAlgorithm;
        private AsyncBlittableJsonTextWriter _writer;
        private DatabaseSmugglerOptionsServerSide _options;
        private Func<LazyStringValue, bool> _filterMetadataProperty;

        public StreamDestination(Stream stream, JsonOperationContext context, ISmugglerSource source, ExportCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel)
        {
            _stream = stream;
            _context = context;
            _source = source;
            _compressionAlgorithm = compressionAlgorithm;
            _compressionLevel = compressionLevel;
        }

        public ValueTask<IAsyncDisposable> InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, Action<IOperationProgress> onProgress, long buildVersion)
        {
            _outputStream = BackupUtils.GetCompressionStream(_stream, _compressionAlgorithm, _compressionLevel);
            _writer = new AsyncBlittableJsonTextWriter(_context, _outputStream);
            _options = options;

            SetupMetadataFilterMethod(_context);
            if (options.IsShard == false)
            {
                _writer.WriteStartObject();
                _writer.WritePropertyName("BuildVersion");
                _writer.WriteInteger(buildVersion);
            }

            return ValueTask.FromResult(InitializeAsyncDispose());
        }

        private IAsyncDisposable InitializeAsyncDispose()
        {
            return new AsyncDisposableAction(async () =>
            {
                if (_options.IsShard == false)
                {
                    _writer.WriteEndObject();
                }

                await _writer.DisposeAsync();
                await _outputStream.DisposeAsync();
            });
        }

        private void SetupMetadataFilterMethod(JsonOperationContext context)
        {
            var skipCountersMetadata = _options.OperateOnTypes.HasFlag(DatabaseItemType.CounterGroups) == false;
            var skipAttachmentsMetadata = _options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments) == false;
            var skipTimeSeriesMetadata = _options.OperateOnTypes.HasFlag(DatabaseItemType.TimeSeries) == false;

            var flags = 0;
            if (skipCountersMetadata)
                flags += 1;
            if (skipAttachmentsMetadata)
                flags += 2;
            if (skipTimeSeriesMetadata)
                flags += 4;

            if (flags == 0)
                return;

            var counters = context.GetLazyString(Constants.Documents.Metadata.Counters);
            var attachments = context.GetLazyString(Constants.Documents.Metadata.Attachments);
            var timeSeries = context.GetLazyString(Constants.Documents.Metadata.TimeSeries);

            switch (flags)
            {
                case 1: // counters
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters);
                    break;

                case 2: // attachments
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(attachments);
                    break;

                case 3: // counters, attachments
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters) || metadataProperty.Equals(attachments);
                    break;

                case 4: // timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(timeSeries);
                    break;

                case 5: // counters, timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters) || metadataProperty.Equals(timeSeries);
                    break;

                case 6: // attachments, timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(attachments) || metadataProperty.Equals(timeSeries);
                    break;

                case 7: // counters, attachments, timeseries
                    _filterMetadataProperty = metadataProperty => metadataProperty.Equals(counters) || metadataProperty.Equals(attachments) || metadataProperty.Equals(timeSeries);
                    break;

                default:
                    throw new NotSupportedException($"Not supported value: {flags}");
            }
        }

        public IDatabaseRecordActions DatabaseRecord()
        {
            return new DatabaseRecordActions(_writer, _context);
        }

        public IDocumentActions Documents(bool throwOnDuplicateCollection)
        {
            return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, "Docs");
        }

        public IDocumentActions RevisionDocuments()
        {
            return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, nameof(DatabaseItemType.RevisionDocuments));
        }

        public IDocumentActions Tombstones()
        {
            return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, nameof(DatabaseItemType.Tombstones));
        }

        public IDocumentActions Conflicts()
        {
            return new StreamDocumentActions(_writer, _context, _source, _options, _filterMetadataProperty, nameof(DatabaseItemType.Conflicts));
        }

        public IKeyValueActions<long> Identities()
        {
            return new StreamKeyValueActions<long>(_writer, nameof(DatabaseItemType.Identities));
        }

        public ICompareExchangeActions CompareExchange(string databaseName, JsonOperationContext context, BackupKind? backupKind, bool withDocuments)
        {
            return withDocuments ? null : new StreamCompareExchangeActions(_writer, context, nameof(DatabaseItemType.CompareExchange));
        }

        public ICompareExchangeActions CompareExchangeTombstones(string databaseName, JsonOperationContext context)
        {
            return new StreamCompareExchangeActions(_writer, context, nameof(DatabaseItemType.CompareExchangeTombstones));
        }

        public ICounterActions Counters(SmugglerResult result)
        {
            return new StreamCounterActions(_writer, _context, this, nameof(DatabaseItemType.CounterGroups));
        }

        public ISubscriptionActions Subscriptions()
        {
            return new StreamSubscriptionActions(_writer, _context, nameof(DatabaseItemType.Subscriptions));
        }

        public IReplicationHubCertificateActions ReplicationHubCertificates()
        {
            return new StreamReplicationHubCertificateActions(_writer, _context, nameof(DatabaseItemType.ReplicationHubCertificates));
        }

        public ITimeSeriesActions TimeSeries()
        {
            return new StreamTimeSeriesActions(_writer, _context, nameof(DatabaseItemType.TimeSeries));
        }

        public ITimeSeriesActions TimeSeriesDeletedRanges()
        {
            return new StreamTimeSeriesActions(_writer, _context, nameof(DatabaseItemType.TimeSeriesDeletedRanges));
        }

        public IIndexActions Indexes()
        {
            return new StreamIndexActions(_writer, _context);
        }

        public ILegacyActions LegacyDocumentDeletions()
        {
            return new StreamLegacyActions(_writer, "DocsDeletions");
        }

        public ILegacyActions LegacyAttachmentDeletions()
        {
            return new StreamLegacyActions(_writer, "AttachmentsDeletions");
        }

        private sealed class DatabaseRecordActions : IDatabaseRecordActions
        {
            private readonly AsyncBlittableJsonTextWriter _writer;
            private readonly JsonOperationContext _context;

            public DatabaseRecordActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
            {
                _writer = writer;
                _context = context;

                _writer.WriteComma();
                _writer.WritePropertyName(nameof(DatabaseItemType.DatabaseRecord));
                _writer.WriteStartObject();
            }

            public async ValueTask WriteDatabaseRecordAsync(DatabaseRecord databaseRecord, SmugglerResult result, AuthorizationStatus authorizationStatus, DatabaseRecordItemType databaseRecordItemType)
            {
                _writer.WritePropertyName(nameof(databaseRecord.DatabaseName));
                _writer.WriteString(databaseRecord.DatabaseName);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(databaseRecord.Encrypted));
                _writer.WriteBool(databaseRecord.Encrypted);
                _writer.WriteComma();

                _writer.WriteArray(nameof(databaseRecord.UnusedDatabaseIds), databaseRecord.UnusedDatabaseIds);

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.LockMode))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.LockMode));
                    _writer.WriteString(databaseRecord.LockMode.ToString());
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.ConflictSolverConfig))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.ConflictSolverConfig));
                    WriteConflictSolver(databaseRecord.ConflictSolverConfig);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Settings))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Settings));
                    WriteSettings(databaseRecord.Settings);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Revisions))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Revisions));
                    WriteRevisions(databaseRecord.Revisions);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.TimeSeries))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.TimeSeries));
                    WriteTimeSeries(databaseRecord.TimeSeries);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.DocumentsCompression))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.DocumentsCompression));
                    WriteDocumentsCompression(databaseRecord.DocumentsCompression);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Expiration))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Expiration));
                    WriteExpiration(databaseRecord.Expiration);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.DataArchival))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.DataArchival));
                    WriteDataArchival(databaseRecord.DataArchival);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.RetireAttachments))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.RetiredAttachments));
                    WriteTaskConfiguration(databaseRecord.RetiredAttachments);
                }
                
                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Refresh))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Refresh));
                    WriteRefresh(databaseRecord.Refresh);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Client))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Client));
                    WriteClientConfiguration(databaseRecord.Client);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Sorters))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Sorters));
                    WriteSorters(databaseRecord.Sorters);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.Analyzers))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Analyzers));
                    WriteAnalyzers(databaseRecord.Analyzers);
                }

                if (databaseRecordItemType.Contain(DatabaseRecordItemType.IndexesHistory))
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.IndexesHistory));
                    WriteIndexesHistory(databaseRecord.IndexesHistory);
                }

                if (databaseRecord.Studio != null)
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Studio));

                    WriteStudioConfiguration(databaseRecord.Studio);
                }

                if (databaseRecord.RevisionsForConflicts != null)
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.RevisionsForConflicts));

                    WriteRevisionsForConflictsConfiguration(databaseRecord.RevisionsForConflicts);
                }

                if (databaseRecord.IsSharded)
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(databaseRecord.Sharding));
                    WriteShardingConfiguration(databaseRecord.Sharding);
                }

                switch (authorizationStatus)
                {
                    case AuthorizationStatus.DatabaseAdmin:
                    case AuthorizationStatus.Operator:
                    case AuthorizationStatus.ClusterAdmin:
                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.RavenConnectionStrings))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.RavenConnectionStrings));
                            WriteRavenConnectionStrings(databaseRecord.RavenConnectionStrings);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.SqlConnectionStrings))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.SqlConnectionStrings));
                            WriteSqlConnectionStrings(databaseRecord.SqlConnectionStrings);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.PeriodicBackups))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.PeriodicBackups));
                            WritePeriodicBackups(databaseRecord.PeriodicBackups);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.ExternalReplications))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.ExternalReplications));
                            WriteExternalReplications(databaseRecord.ExternalReplications);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.RavenEtls))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.RavenEtls));
                            WriteRavenEtls(databaseRecord.RavenEtls);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.SqlEtls))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.SqlEtls));
                            WriteSqlEtls(databaseRecord.SqlEtls);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.HubPullReplications))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.HubPullReplications));
                            WriteHubPullReplications(databaseRecord.HubPullReplications);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.SinkPullReplications))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.SinkPullReplications));
                            WriteSinkPullReplications(databaseRecord.SinkPullReplications);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.OlapConnectionStrings))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.OlapConnectionStrings));
                            WriteOlapConnectionStrings(databaseRecord.OlapConnectionStrings);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.OlapEtls))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.OlapEtls));
                            WriteOlapEtls(databaseRecord.OlapEtls);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.ElasticSearchConnectionStrings))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.ElasticSearchConnectionStrings));
                            WriteElasticSearchConnectionStrings(databaseRecord.ElasticSearchConnectionStrings);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.ElasticSearchEtls))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.ElasticSearchEtls));
                            WriteElasticSearchEtls(databaseRecord.ElasticSearchEtls);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.QueueConnectionStrings))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.QueueConnectionStrings));
                            WriteQueueConnectionStrings(databaseRecord.QueueConnectionStrings);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.QueueEtls))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.QueueEtls));
                            WriteQueueEtls(databaseRecord.QueueEtls);
                        }

                        if (databaseRecordItemType.Contain(DatabaseRecordItemType.QueueSinks))
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.QueueSinks));
                            WriteQueueSinks(databaseRecord.QueueSinks);
                        }

                        if (databaseRecord.Integrations != null)
                        {
                            _writer.WriteComma();
                            _writer.WritePropertyName(nameof(databaseRecord.Integrations));

                            _writer.WriteStartObject();

                            if (databaseRecordItemType.Contain(DatabaseRecordItemType.PostgreSQLIntegration))
                            {
                                _writer.WritePropertyName(nameof(databaseRecord.Integrations.PostgreSql));
                                WritePostgreSqlConfiguration(databaseRecord.Integrations.PostgreSql);
                            }

                            _writer.WriteEndObject();
                        }

                        break;
                }

                await _writer.MaybeFlushAsync();
            }

            private void WriteRevisionsForConflictsConfiguration(RevisionsCollectionConfiguration revisionsForConflictsConfiguration)
            {
                if (revisionsForConflictsConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _context.Write(_writer, revisionsForConflictsConfiguration.ToJson());
            }

            private void WriteStudioConfiguration(StudioConfiguration studioConfiguration)
            {
                if (studioConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _context.Write(_writer, studioConfiguration.ToJson());
            }

            private void WriteHubPullReplications(List<PullReplicationDefinition> hubPullReplications)
            {
                if (hubPullReplications == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();
                var first = true;
                foreach (var pullReplication in hubPullReplications)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _context.Write(_writer, pullReplication.ToJson());
                }
                _writer.WriteEndArray();
            }

            private void WriteSinkPullReplications(List<PullReplicationAsSink> sinkPullReplications)
            {
                if (sinkPullReplications == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();
                var first = true;
                foreach (var pullReplication in sinkPullReplications)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _context.Write(_writer, pullReplication.ToJson());
                }
                _writer.WriteEndArray();
            }

            private void WriteSorters(Dictionary<string, SorterDefinition> sorters)
            {
                if (sorters == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();
                var first = true;
                foreach (var sorter in sorters)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(sorter.Key);
                    _context.Write(_writer, sorter.Value.ToJson());
                }

                _writer.WriteEndObject();
            }

            private void WriteAnalyzers(Dictionary<string, AnalyzerDefinition> analyzers)
            {
                if (analyzers == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();
                var first = true;
                foreach (var analyzer in analyzers)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(analyzer.Key);
                    _context.Write(_writer, analyzer.Value.ToJson());
                }

                _writer.WriteEndObject();
            }

            private void WriteIndexesHistory(Dictionary<string, List<IndexHistoryEntry>> indexesHistory)
            {
                if (indexesHistory == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                var first = true;
                foreach (var historyOfIndex in indexesHistory)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(historyOfIndex.Key);
                    bool isFirstChangeOfIndex = true;

                    _writer.WriteStartArray();
                    foreach (var changeOfIndex in historyOfIndex.Value)
                    {
                        if (isFirstChangeOfIndex == false)
                            _writer.WriteComma();
                        isFirstChangeOfIndex = false;

                        _writer.WriteStartObject();

                        _writer.WritePropertyName(nameof(changeOfIndex.Source));
                        _writer.WriteString(changeOfIndex.Source);
                        _writer.WriteComma();

                        _writer.WritePropertyName(nameof(changeOfIndex.CreatedAt));
                        _writer.WriteDateTime(changeOfIndex.CreatedAt, true);
                        _writer.WriteComma();

                        _writer.WritePropertyName(nameof(changeOfIndex.RollingDeployment));
                        _writer.WriteStartObject();
                        bool isFirstRolling = true;
                        foreach (var (rollingName, rollingIndexDeployment) in changeOfIndex?.RollingDeployment)
                        {
                            if (isFirstRolling == false)
                                _writer.WriteComma();
                            isFirstRolling = false;
                            _writer.WritePropertyName(rollingName);
                            _context.Write(_writer, rollingIndexDeployment.ToJson());
                        }
                        _writer.WriteEndObject();
                        _writer.WriteComma();

                        _writer.WritePropertyName(nameof(changeOfIndex.Definition));
                        _context.Write(_writer, changeOfIndex.Definition.ToJson());

                        _writer.WriteEndObject();
                    }
                    _writer.WriteEndArray();
                }

                _writer.WriteEndObject();
            }


            private static readonly HashSet<string> DoNotBackUp = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                RavenConfiguration.GetKey(x => x.Core.DataDirectory),
                RavenConfiguration.GetKey(x => x.Storage.TempPath),
                RavenConfiguration.GetKey(x => x.Indexing.TempPath),
                RavenConfiguration.GetKey(x => x.Licensing.License),
                RavenConfiguration.GetKey(x => x.Core.RunInMemory)
            };

            private static readonly HashSet<string> ServerWideKeys = DatabaseHelper.GetServerWideOnlyConfigurationKeys().ToHashSet(StringComparer.OrdinalIgnoreCase);

            private void WriteSettings(Dictionary<string, string> settings)
            {
                if (settings == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartArray();
                var first = true;
                foreach (var config in settings)
                {
                    if (!(DoNotBackUp.Contains(config.Key) ||
                          ServerWideKeys.Contains(config.Key)))
                    {
                        if (first == false)
                            _writer.WriteComma();
                        first = false;
                        _writer.WriteStartObject();
                        _writer.WritePropertyName(config.Key);
                        _writer.WriteString(config.Value);
                        _writer.WriteEndObject();
                    }
                }
                _writer.WriteEndArray();
            }

            private void WriteSqlEtls(List<SqlEtlConfiguration> sqlEtlConfiguration)
            {
                if (sqlEtlConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;
                foreach (var etl in sqlEtlConfiguration)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;
                    _context.Write(_writer, etl.ToJson());
                }

                _writer.WriteEndArray();
            }

            private void WriteRavenEtls(List<RavenEtlConfiguration> ravenEtlConfiguration)
            {
                if (ravenEtlConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;
                foreach (var etl in ravenEtlConfiguration)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;
                    _context.Write(_writer, etl.ToJson());
                }

                _writer.WriteEndArray();
            }

            private void WriteOlapEtls(List<OlapEtlConfiguration> olapEtlConfiguration)
            {
                if (olapEtlConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;
                foreach (var etl in olapEtlConfiguration)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;
                    _context.Write(_writer, etl.ToJson());
                }

                _writer.WriteEndArray();
            }

            private void WriteElasticSearchEtls(List<ElasticSearchEtlConfiguration> elasticSearchEtlConfiguration)
            {
                if (elasticSearchEtlConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;
                foreach (var etl in elasticSearchEtlConfiguration)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;
                    _context.Write(_writer, etl.ToJson());
                }

                _writer.WriteEndArray();
            }

            private void WriteQueueSinks(List<QueueSinkConfiguration> queueSinkConfiguration)
            {
                if (queueSinkConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;
                foreach (var configuration in queueSinkConfiguration)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;
                    _context.Write(_writer, configuration.ToJson());
                }

                _writer.WriteEndArray();
            }

            private void WriteQueueEtls(List<QueueEtlConfiguration> queueEtlConfiguration)
            {
                if (queueEtlConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;
                foreach (var etl in queueEtlConfiguration)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;
                    _context.Write(_writer, etl.ToJson());
                }

                _writer.WriteEndArray();
            }

            private void WriteExternalReplications(List<ExternalReplication> externalReplication)
            {
                if (externalReplication == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;
                foreach (var replication in externalReplication)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _context.Write(_writer, replication.ToJson());
                }

                _writer.WriteEndArray();
            }

            private void WritePeriodicBackups(List<PeriodicBackupConfiguration> periodicBackup)
            {
                if (periodicBackup == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _writer.WriteStartArray();

                var first = true;

                foreach (var backup in periodicBackup)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;
                    _context.Write(_writer, backup.ToJson());
                }
                _writer.WriteEndArray();
            }

            private void WriteConflictSolver(ConflictSolver conflictSolver)
            {
                if (conflictSolver == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _context.Write(_writer, conflictSolver.ToJson());
            }

            private void WriteClientConfiguration(ClientConfiguration clientConfiguration)
            {
                if (clientConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _context.Write(_writer, clientConfiguration.ToJson());
            }

            private void WriteExpiration(ExpirationConfiguration expiration)
            {
                if (expiration == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _context.Write(_writer, expiration.ToJson());
            }

            private void WriteDataArchival(DataArchivalConfiguration dataArchival)
            {
                if (dataArchival == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _context.Write(_writer, dataArchival.ToJson());
            }

            private void WriteTaskConfiguration(IDynamicJson config)
            {
                if (config == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _context.Write(_writer, config.ToJson());
            }
            private void WriteRefresh(RefreshConfiguration refresh)
            {
                if (refresh == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _context.Write(_writer, refresh.ToJson());
            }

            private void WriteRevisions(RevisionsConfiguration revisions)
            {
                if (revisions == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _context.Write(_writer, revisions.ToJson());
            }

            private void WriteTimeSeries(TimeSeriesConfiguration timeSeries)
            {
                if (timeSeries == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _context.Write(_writer, timeSeries.ToJson());
            }

            private void WriteDocumentsCompression(DocumentsCompressionConfiguration compressionConfiguration)
            {
                if (compressionConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _context.Write(_writer, compressionConfiguration.ToJson());
            }

            private void WriteRavenConnectionStrings(Dictionary<string, RavenConnectionString> connections)
            {
                _writer.WriteStartObject();

                var first = true;
                foreach (var ravenConnectionString in connections)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(ravenConnectionString.Key);

                    _context.Write(_writer, ravenConnectionString.Value.ToJson());
                }

                _writer.WriteEndObject();
            }

            private void WriteSqlConnectionStrings(Dictionary<string, SqlConnectionString> connections)
            {
                _writer.WriteStartObject();

                var first = true;
                foreach (var sqlConnectionString in connections)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(sqlConnectionString.Key);

                    _context.Write(_writer, sqlConnectionString.Value.ToJson());
                }

                _writer.WriteEndObject();
            }

            private void WriteOlapConnectionStrings(Dictionary<string, OlapConnectionString> connections)
            {
                _writer.WriteStartObject();

                var first = true;
                foreach (var olapConnectionString in connections)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(olapConnectionString.Key);

                    _context.Write(_writer, olapConnectionString.Value.ToJson());
                }

                _writer.WriteEndObject();
            }

            private void WriteElasticSearchConnectionStrings(Dictionary<string, ElasticSearchConnectionString> connections)
            {
                _writer.WriteStartObject();

                var first = true;
                foreach (var elasticSearchConnectionString in connections)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(elasticSearchConnectionString.Key);

                    _context.Write(_writer, elasticSearchConnectionString.Value.ToJson());
                }

                _writer.WriteEndObject();
            }

            private void WriteQueueConnectionStrings(Dictionary<string, QueueConnectionString> connections)
            {
                _writer.WriteStartObject();

                var first = true;
                foreach (var queueConnectionString in connections)
                {
                    if (first == false)
                        _writer.WriteComma();
                    first = false;

                    _writer.WritePropertyName(queueConnectionString.Key);

                    _context.Write(_writer, queueConnectionString.Value.ToJson());
                }

                _writer.WriteEndObject();
            }

            private void WritePostgreSqlConfiguration(PostgreSqlConfiguration postgreSqlConfig)
            {
                if (postgreSqlConfig == null)
                {
                    _writer.WriteNull();
                    return;
                }
                _context.Write(_writer, postgreSqlConfig.ToJson());
            }

            private void WriteShardingConfiguration(ShardingConfiguration shardingConfiguration)
            {
                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(ShardingConfiguration.Shards));
                _context.Write(_writer, DynamicJsonValue.Convert(shardingConfiguration.Shards));

                _writer.WritePropertyName(nameof(ShardingConfiguration.BucketRanges));
                _context.Write(_writer, new DynamicJsonArray(shardingConfiguration.BucketRanges.Select(x => x.ToJson())));

                _writer.WritePropertyName(nameof(ShardingConfiguration.Prefixed));
                _context.Write(_writer, new DynamicJsonArray(shardingConfiguration.Prefixed.Select(x => x.ToJson())));

                _writer.WriteEndObject();
            }

            public ValueTask DisposeAsync()
            {
                _writer.WriteEndObject();
                return default;
            }
        }

        private sealed class StreamIndexActions : StreamActionsBase, IIndexActions
        {
            private readonly JsonOperationContext _context;

            public StreamIndexActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
                : base(writer, "Indexes")
            {
                _context = context;
            }

            public async ValueTask WriteAutoIndexAsync(IndexDefinitionBaseServerSide indexDefinition, IndexType indexType, AuthorizationStatus authorizationStatus)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(nameof(IndexDefinition.Type));
                Writer.WriteString(indexType.ToString());
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(IndexDefinition));
                indexDefinition.Persist(_context, Writer);

                Writer.WriteEndObject();

                await Writer.MaybeFlushAsync();
            }

            public async ValueTask WriteIndexAsync(IndexDefinition indexDefinition, AuthorizationStatus authorizationStatus)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(nameof(IndexDefinition.Type));
                Writer.WriteString(indexDefinition.Type.ToString());
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(IndexDefinition));
                Writer.WriteIndexDefinition(_context, indexDefinition);

                Writer.WriteEndObject();

                await Writer.MaybeFlushAsync();
            }
        }

        private sealed class StreamCounterActions : StreamActionsBaseWithBuilder, ICounterActions
        {
            private readonly JsonOperationContext _context;
            private readonly StreamDestination _destination;
            public async ValueTask WriteCounterAsync(CounterGroupDetail counterDetail)
            {
                CountersStorage.ConvertFromBlobToNumbers(_context, counterDetail);

                using (counterDetail)
                {
                    if (First == false)
                        Writer.WriteComma();
                    First = false;

                    Writer.WriteStartObject();

                    Writer.WritePropertyName(nameof(CounterItem.DocId));
                    Writer.WriteString(counterDetail.DocumentId, skipEscaping: true);
                    Writer.WriteComma();

                    Writer.WritePropertyName(nameof(CounterItem.ChangeVector));
                    Writer.WriteString(counterDetail.ChangeVector, skipEscaping: true);
                    Writer.WriteComma();

                    Writer.WritePropertyName(nameof(CounterItem.Batch.Values));
                    Writer.WriteObject(counterDetail.Values);

                    Writer.WriteEndObject();

                    await Writer.MaybeFlushAsync();
                }
            }

            public async ValueTask WriteLegacyCounterAsync(CounterDetail counterDetail)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName("DocId");
                Writer.WriteString(counterDetail.DocumentId);
                Writer.WriteComma();

                Writer.WritePropertyName("Name");
                Writer.WriteString(counterDetail.CounterName);
                Writer.WriteComma();

                Writer.WritePropertyName("Value");
                Writer.WriteDouble(counterDetail.TotalValue);
                Writer.WriteComma();

                Writer.WritePropertyName("ChangeVector");
                Writer.WriteString(counterDetail.ChangeVector);

                Writer.WriteEndObject();
                await Writer.MaybeFlushAsync();
            }

            public void RegisterForDisposal(IDisposable data)
            {

            }

            public StreamCounterActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, StreamDestination destination, string propertyName) : base(context, writer, propertyName)
            {
                _context = context;
                _destination = destination;
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            public BlittableJsonDocumentBuilder GetBuilderForNewDocument(UnmanagedJsonParser parser, JsonParserState state, BlittableMetadataModifier modifier = null)
            {
                return GetOrCreateBuilder(parser, state, "stream/object", modifier);
            }

            public BlittableMetadataModifier GetMetadataModifierForNewDocument(string firstEtagOfLegacyRevision = null, long legacyRevisionsCount = 0, bool legacyImport = false,
                bool readLegacyEtag = false, DatabaseItemType operateOnTypes = DatabaseItemType.None)
            {
                return GetOrCreateMetadataModifier(firstEtagOfLegacyRevision, legacyRevisionsCount, legacyImport, readLegacyEtag, operateOnTypes);
            }

            public Task<Stream> GetTempStreamAsync()
            {
                throw new NotSupportedException("GetTempStream is never used in StreamCounterActions. Shouldn't happen");
            }
        }

        private sealed class StreamTimeSeriesActions : StreamActionsBaseWithBuilder, ITimeSeriesActions
        {
            private readonly JsonOperationContext _context;

            public StreamTimeSeriesActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, string propertyName) : base(context, writer, propertyName)
            {
                _context = context;
            }

            public async ValueTask WriteTimeSeriesAsync(TimeSeriesItem item)
            {
                using (item)
                {
                    if (First == false)
                        Writer.WriteComma();
                    First = false;

                    Writer.WriteStartObject();
                    {
                        Writer.WritePropertyName(Constants.Documents.Blob.Document);

                        Writer.WriteStartObject();
                        {
                            Writer.WritePropertyName(nameof(TimeSeriesItem.DocId));
                            Writer.WriteString(item.DocId);
                            Writer.WriteComma();

                            Writer.WritePropertyName(nameof(TimeSeriesItem.Name));
                            Writer.WriteString(item.Name);
                            Writer.WriteComma();

                            Writer.WritePropertyName(nameof(TimeSeriesItem.ChangeVector));
                            Writer.WriteString(item.ChangeVector);
                            Writer.WriteComma();

                            Writer.WritePropertyName(nameof(TimeSeriesItem.Collection));
                            Writer.WriteString(item.Collection);
                            Writer.WriteComma();

                            Writer.WritePropertyName(nameof(TimeSeriesItem.Baseline));
                            Writer.WriteDateTime(item.Baseline, true);
                        }
                        Writer.WriteEndObject();

                        Writer.WriteComma();
                        Writer.WritePropertyName(Constants.Documents.Blob.Size);
                        Writer.WriteInteger(item.SegmentSize);
                    }
                    Writer.WriteEndObject();

                    unsafe
                    {
                        Writer.WriteMemoryChunk(item.Segment.Ptr, item.Segment.NumberOfBytes);
                    }

                    await Writer.MaybeFlushAsync();
                }
            }

            public async ValueTask WriteTimeSeriesDeletedRangeAsync(TimeSeriesDeletedRangeItemForSmuggler deletedRangeItem)
            {
                using (deletedRangeItem)
                {
                    if (First == false)
                        Writer.WriteComma();

                    First = false;

                    Writer.WriteStartObject();

                    Writer.WritePropertyName(nameof(TimeSeriesDeletedRangeItemForSmuggler.DocId));
                    Writer.WriteString(deletedRangeItem.DocId, skipEscaping: true);
                    Writer.WriteComma();

                    Writer.WritePropertyName(nameof(TimeSeriesDeletedRangeItemForSmuggler.Name));
                    Writer.WriteString(deletedRangeItem.Name, skipEscaping: true);
                    Writer.WriteComma();

                    Writer.WritePropertyName(nameof(TimeSeriesDeletedRangeItemForSmuggler.Collection));
                    Writer.WriteString(deletedRangeItem.Collection);
                    Writer.WriteComma();

                    Writer.WritePropertyName(nameof(TimeSeriesDeletedRangeItemForSmuggler.ChangeVector));
                    Writer.WriteString(deletedRangeItem.ChangeVector);
                    Writer.WriteComma();

                    Writer.WritePropertyName(nameof(TimeSeriesDeletedRangeItemForSmuggler.From));
                    Writer.WriteDateTime(deletedRangeItem.From, isUtc: true);
                    Writer.WriteComma();

                    Writer.WritePropertyName(nameof(TimeSeriesDeletedRangeItemForSmuggler.To));
                    Writer.WriteDateTime(deletedRangeItem.To, isUtc: true);

                    Writer.WriteEndObject();

                    await Writer.MaybeFlushAsync();
                }
            }

            public void RegisterForDisposal(IDisposable data)
            {
                throw new NotSupportedException($"{nameof(RegisterForDisposal)} is never used in {nameof(StreamTimeSeriesActions)}. Shouldn't happen.");
            }

            public void RegisterForReturnToTheContext(AllocatedMemoryData data)
            {
                throw new NotSupportedException($"{nameof(RegisterForReturnToTheContext)} is never used in {nameof(StreamTimeSeriesActions)}. Shouldn't happen.");
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            public BlittableJsonDocumentBuilder GetBuilderForNewDocument(UnmanagedJsonParser parser, JsonParserState state, BlittableMetadataModifier modifier = null)
            {
                return GetOrCreateBuilder(parser, state, "stream/object", modifier);
            }

            public BlittableMetadataModifier GetMetadataModifierForNewDocument(string firstEtagOfLegacyRevision = null, long legacyRevisionsCount = 0, bool legacyImport = false,
                bool readLegacyEtag = false, DatabaseItemType operateOnTypes = DatabaseItemType.None)
            {
                return GetOrCreateMetadataModifier(firstEtagOfLegacyRevision, legacyRevisionsCount, legacyImport, readLegacyEtag, operateOnTypes);
            }
        }

        private sealed class StreamSubscriptionActions : StreamActionsBase, ISubscriptionActions
        {
            private readonly JsonOperationContext _context;
            private readonly AbstractBlittableJsonTextWriter _writer;

            public StreamSubscriptionActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, string propertyName) : base(writer, propertyName)
            {
                _context = context;
                _writer = writer;
            }

            public async ValueTask WriteSubscriptionAsync(SubscriptionState subscriptionState)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                _context.Write(_writer, subscriptionState.ToJson());

                await Writer.MaybeFlushAsync();
            }
        }

        private sealed class StreamReplicationHubCertificateActions : StreamActionsBase, IReplicationHubCertificateActions
        {
            private readonly JsonOperationContext _context;
            private readonly AsyncBlittableJsonTextWriter _writer;

            public StreamReplicationHubCertificateActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, string propertyName) : base(writer, propertyName)
            {
                _context = context;
                _writer = writer;
            }

            public async ValueTask WriteReplicationHubCertificateAsync(string hub, ReplicationHubAccess access)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                var djv = access.ToJson();
                djv[nameof(RegisterReplicationHubAccessCommand.HubName)] = hub;
                _context.Write(_writer, djv);

                await Writer.MaybeFlushAsync();
            }
        }

        private sealed class StreamDocumentActions : StreamActionsBaseWithBuilder, IDocumentActions
        {
            private readonly JsonOperationContext _context;
            private readonly ISmugglerSource _source;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            private readonly Func<LazyStringValue, bool> _filterMetadataProperty;
            private HashSet<string> _attachmentStreamsAlreadyExported;
            private Stream _attachmentStreamsTempFile;

            public StreamDocumentActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, ISmugglerSource source, DatabaseSmugglerOptionsServerSide options, Func<LazyStringValue, bool> filterMetadataProperty, string propertyName)
                : base(context, writer, propertyName)
            {
                _context = context;
                _source = source;
                _options = options;
                _filterMetadataProperty = filterMetadataProperty;
            }

            public async ValueTask WriteDocumentAsync(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress, Func<ValueTask> beforeFlush)
            {
                var document = item.Document;
                using (document)
                {
                    if (_options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments))
                    {
                        if (item.Attachments != null)
                        {
                            foreach (var attachment in item.Attachments)
                            {
                                attachment.Stream.Position = 0;
                                await WriteAttachmentStreamAsync(attachment.Base64Hash.Content.ToString(), attachment.Stream, attachment.Tag.ToString());
                            }
                        }
                        else
                        {
                            await WriteUniqueAttachmentStreamsAsync(document, progress);
                        }
                    }

                    if (First == false)
                        Writer.WriteComma();
                    First = false;

                    Writer.WriteDocument(_context, document, metadataOnly: false, _filterMetadataProperty);

                    await Writer.MaybeFlushAsync();
                }
            }

            public async ValueTask WriteTombstoneAsync(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                using (tombstone)
                {
                    unsafe
                    {
                        using (var escapedId = _context.GetLazyString(tombstone.LowerId.Buffer, tombstone.LowerId.Size))
                        {
                            _context.Write(Writer, new DynamicJsonValue
                            {
                                ["Key"] = escapedId,
                                [nameof(Tombstone.Type)] = tombstone.Type.ToString(),
                                [nameof(Tombstone.Collection)] = tombstone.Collection,
                                [nameof(Tombstone.Flags)] = tombstone.Flags.ToString(),
                                [nameof(Tombstone.ChangeVector)] = tombstone.ChangeVector,
                                [nameof(Tombstone.DeletedEtag)] = tombstone.DeletedEtag,
                                [nameof(Tombstone.Etag)] = tombstone.Etag,
                                [nameof(Tombstone.LastModified)] = tombstone.LastModified,
                            });


                        }
                    }

                    await Writer.MaybeFlushAsync();
                }
            }

            public async ValueTask WriteConflictAsync(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                using (conflict)
                {
                    _context.Write(Writer, new DynamicJsonValue
                    {
                        [nameof(DocumentConflict.Id)] = conflict.Id,
                        [nameof(DocumentConflict.Collection)] = conflict.Collection,
                        [nameof(DocumentConflict.Flags)] = conflict.Flags.ToString(),
                        [nameof(DocumentConflict.ChangeVector)] = conflict.ChangeVector,
                        [nameof(DocumentConflict.Etag)] = conflict.Etag,
                        [nameof(DocumentConflict.LastModified)] = conflict.LastModified,
                        [nameof(DocumentConflict.Doc)] = conflict.Doc,
                    });

                    await Writer.MaybeFlushAsync();
                }
            }

            public ValueTask DeleteDocumentAsync(string id)
            {
                // no-op
                return default;
            }

            public IEnumerable<DocumentItem> GetDocumentsWithDuplicateCollection()
            {
                yield break;
            }

            public async Task<Stream> GetTempStreamAsync() => _attachmentStreamsTempFile ??= await StreamDestination.GetTempStreamAsync(_options);

            private async ValueTask WriteUniqueAttachmentStreamsAsync(Document document, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress)
            {
                if ((document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments ||
                    document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;

                if (_attachmentStreamsAlreadyExported == null)
                    _attachmentStreamsAlreadyExported = new HashSet<string>();

                foreach (BlittableJsonReaderObject attachment in attachments)
                {
                    if (attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                    {
                        progress.Attachments.ErroredCount++;

                        throw new ArgumentException($"Hash field is mandatory in attachment's metadata: {attachment}");
                    }

                    progress.Attachments.ReadCount++;
                    if (attachment.TryGet(nameof(AttachmentName.Flags), out AttachmentFlags flags) == false || flags == AttachmentFlags.None)
                    {
                        if (_attachmentStreamsAlreadyExported.Add(hash))
                        {
                            await using (var stream = _source.GetAttachmentStream(hash, out string tag))
                            {
                                if (stream == null)
                                {
                                    progress.Attachments.ErroredCount++;
                                    throw new ArgumentException($"Document {document.Id} seems to have an attachment hash: {hash}, but no correlating hash was found in the storage.");
                                }
                                await WriteAttachmentStreamAsync(hash, stream, tag);
                            }
                        }
                    }
                    else if(flags.Contain(AttachmentFlags.Retired))
                    {
                        
                    }


                }
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            public BlittableJsonDocumentBuilder GetBuilderForNewDocument(UnmanagedJsonParser parser, JsonParserState state, BlittableMetadataModifier modifier = null)
            {
                return GetOrCreateBuilder(parser, state, "stream/object", modifier);
            }

            public BlittableMetadataModifier GetMetadataModifierForNewDocument(string firstEtagOfLegacyRevision = null, long legacyRevisionsCount = 0, bool legacyImport = false,
                bool readLegacyEtag = false, DatabaseItemType operateOnTypes = DatabaseItemType.None)
            {
                return GetOrCreateMetadataModifier(firstEtagOfLegacyRevision, legacyRevisionsCount, legacyImport, readLegacyEtag, operateOnTypes);
            }

            private async ValueTask WriteAttachmentStreamAsync(string hash, Stream stream, string tag)
            {
                if (_attachmentStreamsAlreadyExported == null)
                    _attachmentStreamsAlreadyExported = new HashSet<string>();
                _attachmentStreamsAlreadyExported.Add(hash);

                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(Constants.Documents.Metadata.Key);
                Writer.WriteStartObject();

                Writer.WritePropertyName(DocumentItem.ExportDocumentType.Key);
                Writer.WriteString(DocumentItem.ExportDocumentType.Attachment);

                Writer.WriteEndObject();
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(AttachmentName.Hash));
                Writer.WriteString(hash);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(AttachmentName.Size));
                Writer.WriteInteger(stream.Length);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(DocumentItem.AttachmentStream.Tag));
                Writer.WriteString(tag);

                Writer.WriteEndObject();

                await Writer.WriteStreamAsync(stream);

                await Writer.MaybeFlushAsync();
            }

            public override async ValueTask DisposeAsync()
            {
                await base.DisposeAsync();
                await using (_attachmentStreamsTempFile)
                {

                }
            }
        }

        private sealed class StreamKeyValueActions<T> : StreamActionsBase, IKeyValueActions<T>
        {
            public StreamKeyValueActions(AsyncBlittableJsonTextWriter writer, string name)
                : base(writer, name)
            {
            }

            public async ValueTask WriteKeyValueAsync(string key, T value)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();
                Writer.WritePropertyName("Key");
                Writer.WriteString(key);
                Writer.WriteComma();
                Writer.WritePropertyName("Value");
                Writer.WriteString(value.ToString());
                Writer.WriteEndObject();

                await Writer.MaybeFlushAsync();
            }
        }

        private sealed class StreamCompareExchangeActions : StreamActionsBase, ICompareExchangeActions
        {
            private readonly JsonOperationContext _context;
            public StreamCompareExchangeActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, string name)
                : base(writer, name)
            {
                _context = context;
            }

            public async ValueTask WriteKeyValueAsync(string key, BlittableJsonReaderObject value, Document existingDocument)
            {
                using (value)
                {
                    if (First == false)
                        Writer.WriteComma();
                    First = false;

                    Writer.WriteStartObject();
                    Writer.WritePropertyName("Key");
                    Writer.WriteString(key);
                    Writer.WriteComma();
                    Writer.WritePropertyName("Value");
                    Writer.WriteString(value.ToString());
                    Writer.WriteEndObject();

                    await Writer.MaybeFlushAsync();
                }
            }

            public async ValueTask WriteTombstoneKeyAsync(string key)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();
                Writer.WritePropertyName("Key");
                Writer.WriteString(key);
                Writer.WriteEndObject();

                await Writer.MaybeFlushAsync();
            }

            public ValueTask FlushAsync()
            {
                return ValueTask.CompletedTask;
            }
        }

        private abstract class StreamActionsBaseWithBuilder : StreamActionsBase
        {
            protected readonly JsonOperationContext Context;

            private BlittableJsonDocumentBuilder _builder;
            private BlittableMetadataModifier _metadataModifier;

            protected StreamActionsBaseWithBuilder(JsonOperationContext context, AsyncBlittableJsonTextWriter writer, string propertyName)
                : base(writer, propertyName)
            {
                Context = context;
            }

            protected BlittableJsonDocumentBuilder GetOrCreateBuilder(UnmanagedJsonParser parser, JsonParserState state, string debugTag, BlittableMetadataModifier modifier = null)
            {
                return _builder ??= new BlittableJsonDocumentBuilder(Context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, debugTag, parser, state, modifier: modifier);
            }

            protected BlittableMetadataModifier GetOrCreateMetadataModifier(string firstEtagOfLegacyRevision = null, long legacyRevisionsCount = 0, bool legacyImport = false,
                bool readLegacyEtag = false, DatabaseItemType operateOnTypes = DatabaseItemType.None)
            {
                _metadataModifier ??= new BlittableMetadataModifier(Context, legacyImport, readLegacyEtag, operateOnTypes);
                _metadataModifier.FirstEtagOfLegacyRevision = firstEtagOfLegacyRevision;
                _metadataModifier.LegacyRevisionsCount = legacyRevisionsCount;

                return _metadataModifier;
            }

            public override ValueTask DisposeAsync()
            {
                _builder?.Dispose();
                _metadataModifier?.Dispose();

                return base.DisposeAsync();
            }
        }

        private abstract class StreamActionsBase : IAsyncDisposable
        {
            protected readonly AsyncBlittableJsonTextWriter Writer;

            protected bool First { get; set; }

            protected StreamActionsBase(AsyncBlittableJsonTextWriter writer, string propertyName)
            {
                Writer = writer;
                First = true;

                Writer.WriteComma();
                Writer.WritePropertyName(propertyName);
                Writer.WriteStartArray();
            }

            public virtual ValueTask DisposeAsync()
            {
                Writer.WriteEndArray();
                return default;
            }
        }

        private sealed class StreamLegacyActions : StreamActionsBase, ILegacyActions
        {

            public StreamLegacyActions(AsyncBlittableJsonTextWriter writer, string propertyName)
                : base(writer, propertyName)
            {

            }

            public ValueTask WriteLegacyDeletions(string id)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartArray();
                Writer.WritePropertyName("Key");
                Writer.WriteString(id);

                return ValueTask.CompletedTask;
            }
        }

        public static async Task<Stream> GetTempStreamAsync(DatabaseSmugglerOptionsServerSide options)
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.smuggler");
            if (options.EncryptionKey != null)
            {
                var decryptingStream = new DecryptingXChaCha20Oly1305Stream(new StreamsTempFile(tempFileName, true).StartNewStream(), Convert.FromBase64String(options.EncryptionKey));

                await decryptingStream.InitializeAsync();

                return decryptingStream;
            }

            return new StreamsTempFile(tempFileName, false).StartNewStream();
        }
    }
}
