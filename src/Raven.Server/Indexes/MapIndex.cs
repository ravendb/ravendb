using Raven.Server.Documents;
using Raven.Server.Json;

namespace Raven.Server.Indexes
{
    public class MapIndex : Index
    {
        public MapIndex(int indexId, DocumentsStorage documentsStorage)
            : base(indexId, documentsStorage)
        {
        }

        protected override bool IsStale(RavenOperationContext databaseContext, RavenOperationContext indexContext)
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

            return lastDocumentEtag > lastMappedEtag;
        }

        protected override Lucene.Net.Documents.Document ConvertDocument(string collection, Document document)
        {
            throw new System.NotImplementedException();
        }
    }
}