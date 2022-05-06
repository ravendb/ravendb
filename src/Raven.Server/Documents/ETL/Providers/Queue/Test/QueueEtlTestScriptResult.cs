using System.Collections.Generic;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.Queue.Test
{
    public class QueueEtlTestScriptResult : TestEtlScriptResult
    {
        public List<TopicSummary> Summary { get; set; }
    }
}
