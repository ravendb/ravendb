using Raven.Abstractions.Linq;
using Raven.Server.Documents;
using Raven.Server.Json;

namespace Raven.Server.Indexes
{
    public class MapIndex : Index
    {
        public MapIndex(RavenOperationContext context, DocumentsStorage documentsStorage)
            : base(context, documentsStorage)
        {
        }

        protected override bool IsStale()
        {
            return true;
        }

        protected override Lucene.Net.Documents.Document ConvertDocument(string collection, Document document)
        {
            throw new System.NotImplementedException();
        }
    }
}