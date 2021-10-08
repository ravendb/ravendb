using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Test
{
    public class ElasticSearchEtlTestScriptResult : TestEtlScriptResult
    {
        public List<IndexSummary> Summary { get; set; }
    }
}
