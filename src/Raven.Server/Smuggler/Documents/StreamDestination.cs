using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Web.System;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.BTrees;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamDestination : ISmugglerDestination
    {
        private readonly Stream _stream;
        private GZipStream _gzipStream;
        private readonly DocumentsOperationContext _context;
        private readonly DatabaseSource _source;
        private BlittableJsonTextWriter _writer;
        private DatabaseSmugglerOptionsServerSide _options;
        private Func<LazyStringValue, bool> _filterMetadataProperty;

        public StreamDestination(Stream stream, DocumentsOperationContext context, DatabaseSource source)
        {
            _stream = stream;
            _context = context;
            _source = source;
        }

        public IDisposable Initialize(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion)
        {
            _gzipStream = new GZipStream(_stream, CompressionMode.Compress, leaveOpen: true);
            _writer = new BlittableJsonTextWriter(_context, _gzipStream);
            _options = options;

            SetupMetadataFilterMethod(_context);

            _writer.WriteStartObject();

            _writer.WritePropertyName("BuildVersion");
            _writer.WriteInteger(buildVersion);

            return new DisposableAction(() =>
            {
                _writer.WriteEndObject();
                _writer.Dispose();
                _gzipStream.Dispose();
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
            return new StreamCompareExchangeActions(_writer, nameof(DatabaseItemType.CompareExchange));
        }

        public ICompareExchangeActions CompareExchangeTombstones(JsonOperationContext context)
        {
            return new StreamCompareExchangeActions(_writer, nameof(DatabaseItemType.CompareExchangeTombstones));
        }

        public ICounterActions Counters(SmugglerResult result)
        {
            return new StreamCounterActions(_writer, _context, nameof(DatabaseItemType.CounterGroups));
        }

        public ISubscriptionActions Subscriptions()
        {
            return new StreamSubscriptionActions(_writer, _context, nameof(DatabaseItemType.Subscriptions));
        }

        public ITimeSeriesActions TimeSeries()
        {
            return new StreamTimeSeriesActions(_writer, _context, nameof(DatabaseItemType.TimeSeries));
        }

        public IIndexActions Indexes()
        {
            return new StreamIndexActions(_writer, _context);
        }

        private class DatabaseRecordActions : IDatabaseRecordActions
        {
            private readonly BlittableJsonTextWriter _writer;
            private readonly JsonOperationContext _context;

            public DatabaseRecordActions(BlittableJsonTextWriter writer, JsonOperationContext context)
            {
                _writer = writer;
                _context = context;

                _writer.WriteComma();
                _writer.WritePropertyName(nameof(DatabaseItemType.DatabaseRecord));
                _writer.WriteStartObject();
            }

            public void WriteDatabaseRecord(DatabaseRecord databaseRecord, SmugglerProgressBase.DatabaseRecordProgress progress, AuthorizationStatus authorizationStatus, DatabaseRecordItemType databaseRecordItemType)
            {
                _writer.WritePropertyName(nameof(databaseRecord.DatabaseName));
                _writer.WriteString(databaseRecord.DatabaseName);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(databaseRecord.Encrypted));
                _writer.WriteBool(databaseRecord.Encrypted);
                _writer.WriteComma();

                _writer.WriteArray(nameof(databaseRecord.UnusedDatabaseIds), databaseRecord.UnusedDatabaseIds);
                
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

                        break;
                }
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

            public void Dispose()
            {
                _writer.WriteEndObject();
            }
        }

        private class StreamIndexActions : StreamActionsBase, IIndexActions
        {
            private readonly JsonOperationContext _context;

            public StreamIndexActions(BlittableJsonTextWriter writer, JsonOperationContext context)
                : base(writer, "Indexes")
            {
                _context = context;
            }

            public void WriteIndex(IndexDefinitionBaseServerSide indexDefinition, IndexType indexType)
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
            }

            public void WriteIndex(IndexDefinition indexDefinition)
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
            }
        }

        private class StreamCounterActions : StreamActionsBase, ICounterActions
        {
            private readonly DocumentsOperationContext _context;

            public void WriteCounter(CounterGroupDetail counterDetail)
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
                }
            }

            public void WriteLegacyCounter(CounterDetail counterDetail)
            {
                // Used only in Database Destination 
                throw new NotSupportedException("WriteLegacyCounter is not supported when writing to a Stream destination, " +
                                                "it is only supported when writing to Database destination. Shouldn't happen.");
            }

            public void RegisterForDisposal(IDisposable data)
            {
                throw new NotSupportedException("RegisterForDisposal is never used in StreamCounterActions. Shouldn't happen.");
            }

            public StreamCounterActions(BlittableJsonTextWriter writer, DocumentsOperationContext context, string propertyName) : base(writer, propertyName)
            {
                _context = context;
            }

            public DocumentsOperationContext GetContextForNewDocument()
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
            public StreamTimeSeriesActions(BlittableJsonTextWriter writer, DocumentsOperationContext context, string propertyName) : base(writer, propertyName)
            {
            }

            public unsafe void WriteTimeSeries(TimeSeriesItem item)
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

                Writer.WriteMemoryChunk(item.Segment.Ptr, item.Segment.NumberOfBytes);
            }
        }

        private class StreamSubscriptionActions : StreamActionsBase, ISubscriptionActions
        {
            private readonly DocumentsOperationContext _context;
            private readonly BlittableJsonTextWriter _writer;

            public StreamSubscriptionActions(BlittableJsonTextWriter writer, DocumentsOperationContext context, string propertyName) : base(writer, propertyName)
            {
                _context = context;
                _writer = writer;
            }

            public void WriteSubscription(SubscriptionState subscriptionState)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                _context.Write(_writer, subscriptionState.ToJson());
            }
        }

        private class StreamDocumentActions : StreamActionsBase, IDocumentActions
        {
            private readonly DocumentsOperationContext _context;
            private readonly DatabaseSource _source;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            private readonly Func<LazyStringValue, bool> _filterMetadataProperty;
            private HashSet<string> _attachmentStreamsAlreadyExported;

            public StreamDocumentActions(BlittableJsonTextWriter writer, DocumentsOperationContext context, DatabaseSource source, DatabaseSmugglerOptionsServerSide options, Func<LazyStringValue, bool> filterMetadataProperty, string propertyName)
                : base(writer, propertyName)
            {
                _context = context;
                _source = source;
                _options = options;
                _filterMetadataProperty = filterMetadataProperty;
            }

            public void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress)
            {
                if (item.Attachments != null)
                    throw new NotSupportedException();

                var document = item.Document;
                using (document)
                {
                    if (_options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments))
                        WriteUniqueAttachmentStreams(document, progress);

                    if (First == false)
                        Writer.WriteComma();
                    First = false;

                    Writer.WriteDocument(_context, document, metadataOnly: false, _filterMetadataProperty);
                }
            }

            public void WriteTombstone(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                using (tombstone)
                {
                    _context.Write(Writer, new DynamicJsonValue
                    {
                        ["Key"] = tombstone.LowerId,
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

            public void WriteConflict(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
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
                }
            }

            public void DeleteDocument(string id)
            {
                // no-op
            }

            public IEnumerable<DocumentItem> GetDocumentsWithDuplicateCollection()
            {
                yield break;
            }

            public Stream GetTempStream()
            {
                throw new NotSupportedException();
            }

            private void WriteUniqueAttachmentStreams(Document document, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress)
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
                        using (var stream = _source.GetAttachmentStream(hash, out string tag))
                        {
                            if (stream == null)
                            {
                                progress.Attachments.ErroredCount++;
                                throw new ArgumentException($"Document {document.Id} seems to have a attachment hash: {hash}, but no correlating hash was found in the storage.");
                            }
                            WriteAttachmentStream(hash, stream, tag);
                        }
                    }
                }
            }

            public DocumentsOperationContext GetContextForNewDocument()
            {
                _context.CachedProperties.NewDocument();
                return _context;
            }

            private void WriteAttachmentStream(LazyStringValue hash, Stream stream, string tag)
            {
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

                Writer.WriteStream(stream);
            }

        }

        private class StreamKeyValueActions<T> : StreamActionsBase, IKeyValueActions<T>
        {
            public StreamKeyValueActions(BlittableJsonTextWriter writer, string name)
                : base(writer, name)
            {
            }

            public void WriteKeyValue(string key, T value)
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
            }
        }

        private class StreamCompareExchangeActions : StreamActionsBase, ICompareExchangeActions
        {
            public StreamCompareExchangeActions(BlittableJsonTextWriter writer, string name)
                : base(writer, name)
            {
            }

            public void WriteKeyValue(string key, BlittableJsonReaderObject value)
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
                }
            }

            public void WriteTombstoneKey(string key)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();
                Writer.WritePropertyName("Key");
                Writer.WriteString(key);
                Writer.WriteEndObject();
            }
        }

        private abstract class StreamActionsBase : IDisposable
        {
            protected readonly BlittableJsonTextWriter Writer;

            protected bool First { get; set; }

            protected StreamActionsBase(BlittableJsonTextWriter writer, string propertyName)
            {
                Writer = writer;
                First = true;

                Writer.WriteComma();
                Writer.WritePropertyName(propertyName);
                Writer.WriteStartArray();
            }

            public void Dispose()
            {
                Writer.WriteEndArray();
            }
        }
    }
}
