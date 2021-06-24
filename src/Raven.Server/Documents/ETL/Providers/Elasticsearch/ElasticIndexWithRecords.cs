using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.Elasticsearch;

namespace Raven.Server.Documents.ETL.Providers.Elasticsearch
{
    public class ElasticsearchIndexWithRecords : ElasticsearchIndex
    {
        public readonly List<ElasticsearchItem> Deletes = new List<ElasticsearchItem>();

        public readonly List<ElasticsearchItem> Inserts = new List<ElasticsearchItem>();

        public ElasticsearchIndexWithRecords(ElasticsearchIndex index)
        {
            IndexName = index.IndexName;
            IndexIdProperty = index.IndexIdProperty;
        }
    }
}
