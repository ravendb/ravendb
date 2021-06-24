using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.Elasticsearch.Test
{
    public class ElasticsearchEtlTestScriptResult : TestEtlScriptResult
    {
        public List<IndexSummary> Summary { get; set; }
    }
}
