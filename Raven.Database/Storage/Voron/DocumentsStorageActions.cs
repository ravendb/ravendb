using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Voron.Impl;

namespace Raven.Database.Storage.Voron
{
    public class DocumentsStorageActions : IDocumentStorageActions
    {
        private WriteBatch writeBatch;

        private readonly TableStorage storage;
        private readonly IUuidGenerator generator;
        private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;
        private readonly IDocumentCacher documentCacher;

        private readonly Dictionary<Etag, Etag> etagTouches = new Dictionary<Etag, Etag>();

        public DocumentsStorageActions(TableStorage storage,
            IUuidGenerator generator,
            OrderedPartCollection<AbstractDocumentCodec> documentCodecs,
            IDocumentCacher documentCacher,
            WriteBatch writeBatch)
        {
            this.storage = storage;
            this.generator = generator;
            this.documentCodecs = documentCodecs;
            this.documentCacher = documentCacher;
            this.writeBatch = writeBatch;
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
            throw new NotImplementedException();
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
    }
}
