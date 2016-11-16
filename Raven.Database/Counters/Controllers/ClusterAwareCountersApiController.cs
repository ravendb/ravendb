using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Raft.Util;

namespace Raven.Database.Counters.Controllers
{
    public class ClusterAwareCountersApiController : BaseCountersApiController
    {
        protected virtual bool ForceClusterAwareness => false;

        public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
        {
            if (ForceClusterAwareness == false)
            {
                var clusterAwareHeader = GetClusterHeader(controllerContext, Constants.Cluster.ClusterAwareHeader);
                bool clusterAware;
                if (clusterAwareHeader == null || bool.TryParse(clusterAwareHeader, out clusterAware) == false || clusterAware == false)
                    return await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);
            }

            InnerInitialization(controllerContext);

            HttpResponseMessage message;
            if (IsClientV4OrHigher(out message))
                return message;

            if (CounterStorage == null || ClusterManager.IsActive() == false)
                return await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);

            if (CountersName != null && await CountersLandlord.GetResourceInternal(CountersName).ConfigureAwait(false) == null)
            {
                var msg = "Could not find a counter named: " + CountersName;
                return GetMessageWithObject(new { Error = msg }, HttpStatusCode.ServiceUnavailable);
            }

            if (CounterStorage.IsClusterDatabase() == false)
                return await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);

            if (ClusterManager.IsLeader())
                return await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);

            if (IsReadRequest(controllerContext))
            {
                var clusterReadBehaviorHeader = GetClusterHeader(controllerContext, Constants.Cluster.ClusterReadBehaviorHeader);
                if (string.Equals(clusterReadBehaviorHeader, "All", StringComparison.OrdinalIgnoreCase))
                    return await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);
            }

            var clusterFailoverBehaviorHeader = GetClusterHeader(controllerContext, Constants.Cluster.ClusterFailoverBehaviorHeader);
            bool clusterFailoverBehavior;
            if (bool.TryParse(clusterFailoverBehaviorHeader, out clusterFailoverBehavior) && clusterFailoverBehavior)
                return await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);

            return RedirectToLeader(controllerContext.Request);
        }

        private static bool IsReadRequest(HttpControllerContext controllerContext)
        {
            return controllerContext.Request.Method == HttpMethods.Get || controllerContext.Request.Method == HttpMethods.Head;
        }

        private static string GetClusterHeader(HttpControllerContext controllerContext, string key)
        {
            IEnumerable<string> values;
            return controllerContext.Request.Headers.TryGetValues(key, out values) == false ? null : values.FirstOrDefault();
        }

        private HttpResponseMessage RedirectToLeader(HttpRequestMessage request)
        {
            var leaderNode = ClusterManager.Engine.GetLeaderNode();
            if (leaderNode == null)
            {
                return request.CreateResponse(HttpStatusCode.ExpectationFailed, new
                {
                    Error = "There is no current leader, try again later"
                });
            }

            var message = request.CreateResponse(HttpStatusCode.Redirect);
            message.Headers.Location = new UriBuilder(leaderNode.Uri)
            {
                Path = request.RequestUri.LocalPath,
                Query = request.RequestUri.Query.TrimStart('?'),
                Fragment = request.RequestUri.Fragment
            }.Uri;

            return message;
        }
    }
}
