using Microsoft.AspNetCore.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.Utils;

namespace Raven.Server.Web
{
    public sealed class RequestHandlerContext
    {
        public HttpContext HttpContext;
        public RavenServer RavenServer;
        public RouteMatch RouteMatch;
        public DocumentDatabase Database;
        public bool CheckForChanges = true;
        public ShardedDatabaseContext DatabaseContext;

        public string DatabaseName => Database?.Name ?? DatabaseContext?.DatabaseName;
        public MetricCounters DatabaseMetrics => Database?.Metrics ?? DatabaseContext?.Metrics;
        public string ClusterTransactionId => Database?.ClusterTransactionId ?? DatabaseContext?.DatabaseRecord.GetClusterTransactionId();
    }
}
