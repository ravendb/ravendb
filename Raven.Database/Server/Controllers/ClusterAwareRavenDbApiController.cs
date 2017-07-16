// -----------------------------------------------------------------------
//  <copyright file="ClusterAwareRavenDbApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http.Controllers;
using Rachis.Transport;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Raft.Util;

namespace Raven.Database.Server.Controllers
{
    public class ClusterAwareRavenDbApiController : BaseDatabaseApiController
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

            if (Database == null || ClusterManager.IsActive() == false)
                return await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);

            if (DatabaseName != null && await DatabasesLandlord.GetResourceInternal(DatabaseName).ConfigureAwait(false) == null)
            {
                var msg = "Could not find a database named: " + DatabaseName;
                return GetMessageWithObject(new { Error = msg }, HttpStatusCode.ServiceUnavailable);
            }

            if (Database.IsClusterDatabase() == false)
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

            const int NUMBER_OF_REDIRECT_FIND_LEADER_RETRIES = 2;
            

            NodeConnectionInfo leaderNode = null;
            
            for (var leaderSeekRetries = 0; leaderSeekRetries <= NUMBER_OF_REDIRECT_FIND_LEADER_RETRIES && leaderNode==null; leaderSeekRetries++)
            {
                var waitTimeToLeader = leaderSeekRetries * ClusterManager.Engine.Options.ElectionTimeout;

                if (leaderSeekRetries > 0 && Log.IsDebugEnabled)
                    Log.Debug("Redirect To Leader: leader not found, retrying {0} times out of {1}. This time will wait for {2} miliseconds", leaderSeekRetries, NUMBER_OF_REDIRECT_FIND_LEADER_RETRIES, waitTimeToLeader);

                leaderNode = ClusterManager.Engine.GetLeaderNode(waitTimeToLeader);
            }
            
            if (leaderNode == null)
            {
                return request.CreateResponse(HttpStatusCode.ExpectationFailed, new
                {
                    Error = "There is no current leader, try again later"
                });
            }

            var message = request.CreateResponse(HttpStatusCode.Redirect);
            message.Headers.Add("Raven-Leader-Redirect", "true");
            var uriBuilder = new UriBuilder(leaderNode.Uri);
            if (DatabaseName != null)
                uriBuilder.Path = "/databases/" + DatabaseName;
            message.Headers.Location = uriBuilder.Uri;

            return message;
        }
    }
}
