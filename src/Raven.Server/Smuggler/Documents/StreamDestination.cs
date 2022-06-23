using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Analysis;
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
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamDestination : ISmugglerDestination
    {
        private readonly Stream _stream;
        private GZipStream _gzipStream;
        private readonly JsonOperationContext _context;
        private readonly ISmugglerSource _source;
        private AsyncBlittableJsonTextWriter _writer;
        private DatabaseSmugglerOptionsServerSide _options;
        private Func<LazyStringValue, bool> _filterMetadataProperty;
        public StreamDestination(Stream stream, JsonOperationContext context, ISmugglerSource source)
        {
            _stream = stream;
            _context = context;
            _source = source;
        }

        public virtual ValueTask<IAsyncDisposable> InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion)
        {
            _gzipStream = new GZipStream(_stream, CompressionMode.Compress, leaveOpen: true);
            _writer = new AsyncBlittableJsonTextWriter(_context, _gzipStream);
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

        protected virtual IAsyncDisposable InitializeAsyncDispose()
        {
            return new AsyncDisposableAction(async () =>
            {
                if (_options.IsShard == false)
                {
                    _writer.WriteEndObject();
                }

                await _writer.DisposeAsync();
                await _gzipStream.DisposeAsync();
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

        public ICompareExchangeActions CompareExchange(JsonOperationContext context)
        {
            return new StreamCompareExchangeActions(_writer, context, nameof(DatabaseItemType.CompareExchange));
        }

        public ICompareExchangeActions CompareExchangeTombstones(JsonOperationContext context)
        {
            return new StreamCompareExchangeActions(_writer, context, nameof(DatabaseItemType.CompareExchangeTombstones));
        }

        public ICounterActions Counters(SmugglerResult result)
        {
            return new StreamCounterActions(_writer, _context, this, nameof(DatabaseItemType.CounterGroups));
        }
        public ICounterActions LegacyCounters(SmugglerResult result)
        {
#pragma warning disable CS0618
            return new StreamCounterActions(_writer, _context, this, nameof(DatabaseItemType.Counters));
#pragma warning restore CS0618
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

        private class DatabaseRecordActions : IDatabaseRecordActions
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

            public async ValueTask WriteDatabaseRecordAsync(DatabaseRecord databaseRecord, SmugglerProgressBase.DatabaseRecordProgress progress, AuthorizationStatus authorizationStatus, DatabaseRecordItemType databaseRecordItemType)
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

            public ValueTask DisposeAsync()
            {
                _writer.WriteEndObject();
                return default;
            }
        }

        private class StreamIndexActions : StreamActionsBase, IIndexActions
        {
            private readonly JsonOperationContext _context;

            public StreamIndexActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
                : base(writer, "Indexes")
            {
                _context = context;
            }

            public async ValueTask WriteIndexAsync(IndexDefinitionBaseServerSide indexDefinition, IndexType indexType)
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

            public async ValueTask WriteIndexAsync(IndexDefinition indexDefinition)
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

        private class StreamCounterActions : StreamActionsBase, ICounterActions
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

            public StreamCounterActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, StreamDestination destination, string propertyName) : base(writer, propertyName)
            {
                _context = context;
                _destination = destination;
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            public Stream GetTempStream()
            {
                throw new NotSupportedException("GetTempStream is never used in StreamCounterActions. Shouldn't happen");
            }
        }

        private class StreamTimeSeriesActions : StreamActionsBase, ITimeSeriesActions
        {
            public StreamTimeSeriesActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, string propertyName) : base(writer, propertyName)
            {
            }

            public async ValueTask WriteTimeSeriesAsync(TimeSeriesItem item)
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
                    Writer.WriteInteger(item.Segment.NumberOfBytes);
                }
                Writer.WriteEndObject();

                unsafe
                {
                    Writer.WriteMemoryChunk(item.Segment.Ptr, item.Segment.NumberOfBytes);
                }

                await Writer.MaybeFlushAsync();
            }
        }

        private class StreamSubscriptionActions : StreamActionsBase, ISubscriptionActions
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

        private class StreamReplicationHubCertificateActions : StreamActionsBase, IReplicationHubCertificateActions
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

        private class StreamDocumentActions : StreamActionsBase, IDocumentActions
        {
            private readonly JsonOperationContext _context;
            private readonly ISmugglerSource _source;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            private readonly Func<LazyStringValue, bool> _filterMetadataProperty;
            private HashSet<string> _attachmentStreamsAlreadyExported;

            public StreamDocumentActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, ISmugglerSource source, DatabaseSmugglerOptionsServerSide options, Func<LazyStringValue, bool> filterMetadataProperty, string propertyName)
                : base(writer, propertyName)
            {
                _context = context;
                _source = source;
                _options = options;
                _filterMetadataProperty = filterMetadataProperty;
            }

            public async ValueTask WriteDocumentAsync(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress)
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

            public Stream GetTempStream() => StreamDestination.GetTempStream(_options);

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

                    if (_attachmentStreamsAlreadyExported.Add(hash))
                    {
                        await using (var stream = _source.GetAttachmentStream(hash, out string tag))
                        {
                            if (stream == null)
                            {
                                progress.Attachments.ErroredCount++;
                                throw new ArgumentException($"Document {document.Id} seems to have a attachment hash: {hash}, but no correlating hash was found in the storage.");
                            }
                            await WriteAttachmentStreamAsync(hash, stream, tag);
                        }
                    }
                }
            }

            public JsonOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
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
        }

        private class StreamKeyValueActions<T> : StreamActionsBase, IKeyValueActions<T>
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

        private class StreamCompareExchangeActions : StreamActionsBase, ICompareExchangeActions
        {
            private readonly JsonOperationContext _context;
            public StreamCompareExchangeActions(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, string name)
                : base(writer, name)
            {
                _context = context;
            }

            public async ValueTask WriteKeyValueAsync(string key, BlittableJsonReaderObject value)
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

            public JsonOperationContext GetContextForNewCompareExchangeValue()
            {
                return _context;
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

        private class StreamLegacyActions : StreamActionsBase, ILegacyActions
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

        public static Stream GetTempStream(DatabaseSmugglerOptionsServerSide options)
        {
            var tempFileName = $"{Guid.NewGuid()}.smuggler";
            if (options.EncryptionKey != null)
                return new DecryptingXChaCha20Oly1305Stream(new StreamsTempFile(tempFileName, true).StartNewStream(), Convert.FromBase64String(options.EncryptionKey));

            return new StreamsTempFile(tempFileName, false).StartNewStream();
        }
    }
}
