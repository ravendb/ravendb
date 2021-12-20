using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public class ElasticSearchIndexWithRecords : ElasticSearchIndex
    {
        public readonly List<ElasticSearchItem> Deletes = new List<ElasticSearchItem>();

        public readonly List<ElasticSearchItem> Inserts = new List<ElasticSearchItem>();

        public ElasticSearchIndexWithRecords(ElasticSearchIndex index)
        {
            IndexName = index.IndexName;
            DocumentIdProperty = index.DocumentIdProperty;
            InsertOnlyMode = index.InsertOnlyMode;
        }
    }
}
