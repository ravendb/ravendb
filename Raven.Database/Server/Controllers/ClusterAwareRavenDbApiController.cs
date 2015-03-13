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

using Raven.Abstractions.Data;
using Raven.Database.Raft.Util;

namespace Raven.Database.Server.Controllers
{
	public class ClusterAwareRavenDbApiController : RavenDbApiController
	{
		protected virtual bool ForceClusterAwareness
		{
			get
			{
				return false;
			}
		}

		public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
		{
			if (ForceClusterAwareness == false)
			{
				IEnumerable<string> values;
				if (controllerContext.Request.Headers.TryGetValues(Constants.Cluster.ClusterAwareHeader, out values) == false)
					return await base.ExecuteAsync(controllerContext, cancellationToken);

				var clusterAwareHeader = values.FirstOrDefault();
				bool clusterAware;
				if (clusterAwareHeader == null || bool.TryParse(clusterAwareHeader, out clusterAware) == false || clusterAware == false)
					return await base.ExecuteAsync(controllerContext, cancellationToken);
			}

			InnerInitialization(controllerContext);

			if (Database == null || ClusterManager.IsActive() == false)
				return await base.ExecuteAsync(controllerContext, cancellationToken);

			if (DatabaseName != null && await DatabasesLandlord.GetDatabaseInternal(DatabaseName) == null)
			{
				var msg = "Could not find a database named: " + DatabaseName;
				return GetMessageWithObject(new { Error = msg }, HttpStatusCode.ServiceUnavailable);
			}

			if (Database.IsClusterDatabase() == false)
				return await base.ExecuteAsync(controllerContext, cancellationToken);

			if (ClusterManager.IsLeader())
				return await base.ExecuteAsync(controllerContext, cancellationToken);

			return RedirectToLeader(controllerContext.Request);
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