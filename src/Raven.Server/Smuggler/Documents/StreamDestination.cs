using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents
{
    public class StreamDestination : ISmugglerDestination
    {
        private readonly Stream _stream;
        private GZipStream _gzipStream;
        private readonly DocumentsOperationContext _context;
        private readonly DatabaseSource _source;
        private BlittableJsonTextWriter _writer;
        private static DatabaseSmugglerOptions _options;

        public StreamDestination(Stream stream, DocumentsOperationContext context, DatabaseSource source)
        {
            _stream = stream;
            _context = context;
            _source = source;
        }

        public IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, long buildVersion)
        {
            _gzipStream = new GZipStream(_stream, CompressionMode.Compress, leaveOpen: true);
            _writer = new BlittableJsonTextWriter(_context, _gzipStream);
            _options = options;

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

        public IDatabaseRecordActions DatabaseRecord()
        {
            return new DatabaseRecordActions(_writer);
        }

        public IDocumentActions Documents()
        {
            return new StreamDocumentActions(_writer, _context, _source, "Docs", new SmugglerMetadataModifier(_options.OperateOnTypes));
        }

        public IDocumentActions RevisionDocuments()
        {
            return new StreamDocumentActions(_writer, _context, _source, nameof(DatabaseItemType.RevisionDocuments));
        }

        public IDocumentActions Tombstones()
        {
            return new StreamDocumentActions(_writer, _context, _source, nameof(DatabaseItemType.Tombstones));
        }

        public IDocumentActions Conflicts()
        {
            return new StreamDocumentActions(_writer, _context, _source, nameof(DatabaseItemType.Conflicts));
        }

        public IKeyValueActions<long> Identities()
        {
            return new StreamKeyValueActions<long>(_writer, nameof(DatabaseItemType.Identities));
        }

        public IKeyValueActions<BlittableJsonReaderObject> CompareExchange(JsonOperationContext context)
        {
            return new StreamKeyValueActions<BlittableJsonReaderObject>(_writer, nameof(DatabaseItemType.CompareExchange));
        }

        public IKeyActions<long> CompareExchangeTombstones(JsonOperationContext context)
        {
            return new StreamKeyActions<long>(_writer, nameof(DatabaseItemType.CompareExchangeTombstones));
        }

        public ICounterActions Counters()
        {
            return new StreamCounterActions(_writer, nameof(DatabaseItemType.Counters));
        }

        public IIndexActions Indexes()
        {
            return new StreamIndexActions(_writer, _context);
        }

        private class DatabaseRecordActions : IDatabaseRecordActions
        {
            private readonly BlittableJsonTextWriter _writer;

            public DatabaseRecordActions(BlittableJsonTextWriter writer)
            {
                _writer = writer;

                _writer.WriteComma();
                _writer.WritePropertyName(nameof(DatabaseItemType.DatabaseRecord));
                _writer.WriteStartObject();
            }

            public void WriteDatabaseRecord(DatabaseRecord databaseRecord, SmugglerProgressBase.DatabaseRecordProgress progress, AuthorizationStatus authorizationStatus)
            {
                _writer.WritePropertyName(nameof(databaseRecord.DatabaseName));
                _writer.WriteString(databaseRecord.DatabaseName);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(databaseRecord.Encrypted));
                _writer.WriteBool(databaseRecord.Encrypted);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(databaseRecord.Revisions));
                WriteRevisions(databaseRecord.Revisions);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(databaseRecord.Expiration));
                WriteExpiration(databaseRecord.Expiration);
                _writer.WriteComma();

                if (authorizationStatus == AuthorizationStatus.DatabaseAdmin)
                {
                    _writer.WritePropertyName(nameof(databaseRecord.RavenConnectionStrings));
                    WriteRavenConnectionStrings(databaseRecord.RavenConnectionStrings);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(databaseRecord.SqlConnectionStrings));
                    WriteSqlConnectionStrings(databaseRecord.SqlConnectionStrings);
                    _writer.WriteComma();
                }

                _writer.WritePropertyName(nameof(databaseRecord.Client));
                WriteClientConfiguration(databaseRecord.Client);
            }

            private void WriteClientConfiguration(ClientConfiguration clientConfiguration)
            {
                if (clientConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(clientConfiguration.Etag));
                _writer.WriteInteger(clientConfiguration.Etag);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(clientConfiguration.Disabled));
                _writer.WriteBool(clientConfiguration.Disabled);

                if (clientConfiguration.MaxNumberOfRequestsPerSession.HasValue)
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(clientConfiguration.MaxNumberOfRequestsPerSession));
                    _writer.WriteInteger(clientConfiguration.MaxNumberOfRequestsPerSession.Value);
                }

                if (clientConfiguration.PrettifyGeneratedLinqExpressions.HasValue)
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(clientConfiguration.PrettifyGeneratedLinqExpressions));
                    _writer.WriteBool(clientConfiguration.PrettifyGeneratedLinqExpressions.Value);
                }

                if (clientConfiguration.ReadBalanceBehavior.HasValue)
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(clientConfiguration.ReadBalanceBehavior));
                    _writer.WriteString(clientConfiguration.ReadBalanceBehavior.Value.ToString());
                }

                _writer.WriteEndObject();
            }

            private void WriteExpiration(ExpirationConfiguration expiration)
            {
                if (expiration == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(expiration.Disabled));
                _writer.WriteBool(expiration.Disabled);

                if (expiration.DeleteFrequencyInSec.HasValue)
                {
                    _writer.WriteComma();
                    _writer.WritePropertyName(nameof(expiration.DeleteFrequencyInSec));
                    _writer.WriteString(expiration.DeleteFrequencyInSec.Value.ToString());
                }

                _writer.WriteEndObject();
            }

            private void WriteRevisions(RevisionsConfiguration revisions)
            {
                if (revisions == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                _writer.WritePropertyName(nameof(revisions.Default));
                WriteRevisionsCollectionConfiguration(revisions.Default);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(revisions.Collections));

                if (revisions.Collections == null)
                {
                    _writer.WriteNull();
                }
                else
                {
                    _writer.WriteStartObject();

                    var first = true;
                    foreach (var collection in revisions.Collections)
                    {
                        if (first == false)
                            _writer.WriteComma();
                        first = false;

                        _writer.WritePropertyName(collection.Key);
                        WriteRevisionsCollectionConfiguration(collection.Value);
                    }

                    _writer.WriteEndObject();
                }


                _writer.WriteEndObject();
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

                    _writer.WritePropertyName(nameof(ravenConnectionString.Key));

                    _writer.WriteStartObject();

                    var value = ravenConnectionString.Value;
                    _writer.WritePropertyName(nameof(value.Name));
                    _writer.WriteString(value.Name);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(value.Database));
                    _writer.WriteString(value.Database);
                    _writer.WriteComma();

                    _writer.WriteArray(nameof(value.TopologyDiscoveryUrls), value.TopologyDiscoveryUrls);

                    _writer.WriteEndObject();
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

                    _writer.WritePropertyName(nameof(sqlConnectionString.Key));

                    _writer.WriteStartObject();

                    var value = sqlConnectionString.Value;
                    _writer.WritePropertyName(nameof(value.Name));
                    _writer.WriteString(value.Name);
                    _writer.WriteComma();

                    _writer.WritePropertyName(nameof(value.ConnectionString));
                    _writer.WriteString(value.ConnectionString);

                    _writer.WriteEndObject();
                }

                _writer.WriteEndObject();
            }

            private void WriteRevisionsCollectionConfiguration(RevisionsCollectionConfiguration collectionConfiguration)
            {
                if (collectionConfiguration == null)
                {
                    _writer.WriteNull();
                    return;
                }

                _writer.WriteStartObject();

                if (collectionConfiguration.MinimumRevisionsToKeep.HasValue)
                {
                    _writer.WritePropertyName(nameof(collectionConfiguration.MinimumRevisionsToKeep));
                    _writer.WriteInteger(collectionConfiguration.MinimumRevisionsToKeep.Value);
                    _writer.WriteComma();
                }

                if (collectionConfiguration.MinimumRevisionAgeToKeep.HasValue)
                {
                    _writer.WritePropertyName(nameof(collectionConfiguration.MinimumRevisionAgeToKeep));
                    _writer.WriteString(collectionConfiguration.MinimumRevisionAgeToKeep.Value.ToString());
                    _writer.WriteComma();
                }

                _writer.WritePropertyName(nameof(collectionConfiguration.Disabled));
                _writer.WriteBool(collectionConfiguration.Disabled);
                _writer.WriteComma();

                _writer.WritePropertyName(nameof(collectionConfiguration.PurgeOnDelete));
                _writer.WriteBool(collectionConfiguration.PurgeOnDelete);

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

            public void WriteIndex(IndexDefinitionBase indexDefinition, IndexType indexType)
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
            public void WriteCounter(CounterDetail counterDetail)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

                Writer.WriteStartObject();

                Writer.WritePropertyName(nameof(DocumentItem.CounterItem.DocId));
                Writer.WriteString(counterDetail.DocumentId);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(DocumentItem.CounterItem.Name));
                Writer.WriteString(counterDetail.CounterName);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(DocumentItem.CounterItem.Value));
                Writer.WriteInteger(counterDetail.TotalValue);
                Writer.WriteComma();

                Writer.WritePropertyName(nameof(DocumentItem.CounterItem.ChangeVector));
                Writer.WriteString(counterDetail.ChangeVector);

                Writer.WriteEndObject();
            }

            public StreamCounterActions(BlittableJsonTextWriter writer, string propertyName) : base(writer, propertyName)
            {
            }
        }

        private class StreamDocumentActions : StreamActionsBase, IDocumentActions
        {
            private readonly DocumentsOperationContext _context;
            private readonly DatabaseSource _source;
            private HashSet<string> _attachmentStreamsAlreadyExported;
            private readonly IMetadataModifier _modifier;

            public StreamDocumentActions(BlittableJsonTextWriter writer, DocumentsOperationContext context, DatabaseSource source, string propertyName, IMetadataModifier modifier = null)
                : base(writer, propertyName)
            {
                _context = context;
                _source = source;
                _modifier = modifier;
            }

            public void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (item.Attachments != null)
                    throw new NotSupportedException();

                var document = item.Document;
                using (document.Data)
                {
                    if (_options.OperateOnTypes.HasFlag(DatabaseItemType.Attachments))
                        WriteUniqueAttachmentStreams(document, progress);

                    if (First == false)
                        Writer.WriteComma();
                    First = false;

                    document.EnsureMetadata(_modifier);

                    _context.Write(Writer, document.Data);
                }
            }

            public void WriteTombstone(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

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

            public void WriteConflict(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                if (First == false)
                    Writer.WriteComma();
                First = false;

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

            public void DeleteDocument(string id)
            {
                // no-op
            }

            public Stream GetTempStream()
            {
                throw new NotSupportedException();
            }

            private void WriteUniqueAttachmentStreams(Document document, SmugglerProgressBase.CountsWithLastEtag progress)
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

        private class StreamKeyActions<T> : StreamActionsBase, IKeyActions<T>
        {
            public StreamKeyActions(BlittableJsonTextWriter writer, string name)
                : base(writer, name)
            {
            }

            public void WriteKey(string key)
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
