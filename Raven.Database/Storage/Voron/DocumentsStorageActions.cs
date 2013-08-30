namespace Raven.Database.Storage.Voron
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Text;
	using Raven.Abstractions;
	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Abstractions.MEF;
	using Raven.Database.Impl;
	using Raven.Database.Plugins;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Database.Util.Streams;
	using Raven.Json.Linq;
	using Raven.Abstractions.Extensions;

	using global::Voron.Impl;

	public class DocumentsStorageActions : IDocumentStorageActions
    {
        private const string MetadataSuffix = "metadata";
        private const string DataSuffix = "data";

        private readonly Table documentsTable;

        private readonly WriteBatch writeBatch;
        private readonly SnapshotReader snapshot;

        private readonly IUuidGenerator uuidGenerator;
        private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;
        private readonly IDocumentCacher documentCacher;

        private readonly Dictionary<Etag, Etag> etagTouches = new Dictionary<Etag, Etag>();

        public DocumentsStorageActions(IUuidGenerator uuidGenerator,
            OrderedPartCollection<AbstractDocumentCodec> documentCodecs,
            IDocumentCacher documentCacher,
            WriteBatch writeBatch,
            SnapshotReader snapshot,
            Table documentsTable)
        {
            this.snapshot = snapshot;
            this.uuidGenerator = uuidGenerator;
            this.documentCodecs = documentCodecs;
            this.documentCacher = documentCacher;
            this.writeBatch = writeBatch;
            this.documentsTable = documentsTable;
        }


        public IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<JsonDocument> GetDocumentsAfter(Etag etag, int take, long? maxSize = null, Etag untilEtag = null)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take)
        {
            throw new NotImplementedException();
        }

        public long GetDocumentsCount()
        {
            throw new NotImplementedException();
        }

        public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
        {
            var documentStream = documentsTable.Read(snapshot, key);
            throw new NotImplementedException();
        }

        public JsonDocumentMetadata DocumentMetadataByKey(string key, TransactionInformation transactionInformation)
        {
            throw new NotImplementedException();
        }

        public bool DeleteDocument(string key, Etag etag, out RavenJObject metadata, out Etag deletedETag)
        {
            throw new NotImplementedException();
        }

        public AddDocumentResult AddDocument(string key, Etag etag, RavenJObject data, RavenJObject metadata)
        {
            //TODO : do not forget to add document caching
            var isUpdate = documentsTable.Contains(snapshot, key);
            var existingEtag = isUpdate ? EnsureDocumentEtagMatch(key, etag) : Etag.Empty;
            
            var dataStream = new BufferPoolStream(new MemoryStream(),
                new BufferPool(BufferPoolStream.MaxBufferSize * 2, BufferPoolStream.MaxBufferSize));
            var metadataStream = new BufferPoolStream(new MemoryStream(),
                new BufferPool(BufferPoolStream.MaxBufferSize * 2, BufferPoolStream.MaxBufferSize));

            data.WriteTo(dataStream);

            var finalDataStream = documentCodecs.Aggregate((Stream)dataStream,
                (current, codec) => codec.Encode(key, data, metadata, current));

            metadata.WriteTo(metadataStream);

            documentsTable.AddOrUpdate(writeBatch, DataKey(key),finalDataStream);
            documentsTable.AddOrUpdate(writeBatch, MetadataKey(key), metadataStream);

            var newEtag = uuidGenerator.CreateSequentialUuid(UuidType.Documents);

            documentsTable.GetIndex(Tables.Documents.Indices.KeyByEtag)
                          .AddOrUpdate(writeBatch, etag ?? newEtag, Encoding.UTF8.GetBytes(key));

            var savedAt = SystemTime.UtcNow;
            
            return new AddDocumentResult
            {
                Etag = newEtag,
                PrevEtag = existingEtag,
                SavedAt = savedAt,
                Updated = isUpdate
            };            
        }
        
        public AddDocumentResult PutDocumentMetadata(string key, RavenJObject metadata)
        {
            throw new NotImplementedException();
        }

        public void IncrementDocumentCount(int value)
        {
            throw new NotImplementedException();
        }

        public AddDocumentResult InsertDocument(string key, RavenJObject data, RavenJObject metadata, bool checkForUpdates)
        {
            throw new NotImplementedException();
        }

        public void TouchDocument(string key, out Etag preTouchEtag, out Etag afterTouchEtag)
        {
            throw new NotImplementedException();
        }

        public Etag GetBestNextDocumentEtag(Etag etag)
        {
            throw new NotImplementedException();
        }

        private Etag EnsureDocumentEtagMatch(string key, Etag etag)
        {
	        var read = documentsTable.Read(snapshot, MetadataKey(key));

			if (read == null)
				return Etag.InvalidEtag;

            using (var documentStream = read.Stream)
            {
                var etagBuffer = new byte[16];
                documentStream.Read(etagBuffer, 0, 16);
                var existingEtag = Etag.Parse(etagBuffer);
                if (existingEtag != etag)
                {
                    throw new ConcurrencyException(String.Format("Attempted to change document (key = {0}) with non-current etag (etag = {1})", key, etag));
                }


            return existingEtag;
            }
        }

        private static string DataKey(string key)
        {
            return key + "/" + DataSuffix;
        }

        private static string MetadataKey(string key)
        {
            return key + "/" + MetadataSuffix;
        }
    }
}
