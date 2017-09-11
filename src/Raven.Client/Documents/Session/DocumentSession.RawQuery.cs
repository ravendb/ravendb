using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Indexes;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentSession
    {
        public IRawDocumentQuery<T> RawQuery<T>(string query, string indexName = null)
        {
            var documentQuery = new DocumentQuery<T>(this, indexName, null, false);
            documentQuery.RawQuery(query);
            return documentQuery;
        }

        public IRawDocumentQuery<T> RawQuery<T, TIndexCreator>(string query) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var index = new TIndexCreator();
            return RawQuery<T>(query, index.IndexName);
        }
    }
}
