using System;

using Raven.Server.Json;

namespace Raven.Server.Documents.Indexes
{
    public class MapIndex : Index
    {
        public MapIndex(int indexId, DocumentsStorage documentsStorage)
            : this(indexId, IndexType.Map,  documentsStorage)
        {
        }

        protected MapIndex(int indexId, IndexType type, DocumentsStorage documentsStorage)
            : base(indexId, type, documentsStorage)
        {
        }

        protected override string[] Collections
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        protected override bool IsStale(RavenOperationContext databaseContext, RavenOperationContext indexContext, out long lastEtag)
        {
            long lastDocumentEtag;
            using (var tx = databaseContext.Environment.ReadTransaction())
            {
                lastDocumentEtag = DocumentsStorage.ReadLastEtag(tx);
            }

            long lastMappedEtag;
            using (var tx = indexContext.Environment.ReadTransaction())
            {
                lastMappedEtag = ReadLastMappedEtag(tx);
            }

            lastEtag = lastMappedEtag;
            return lastDocumentEtag > lastMappedEtag;
        }

        protected override Lucene.Net.Documents.Document ConvertDocument(string collection, Document document)
        {
            throw new System.NotImplementedException();
        }
    }
}