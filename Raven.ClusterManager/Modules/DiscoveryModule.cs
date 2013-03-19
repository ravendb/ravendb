// -----------------------------------------------------------------------
//  <copyright file="DiscoveryModule.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Nancy;
using Raven.Client;
using Raven.ClusterManager.Discovery;
using Raven.ClusterManager.Models;
using Nancy.ModelBinding;
using System.Linq;
using Raven.ClusterManager.Tasks;

namespace Raven.ClusterManager.Modules
{
	public class DiscoveryModule : NancyModule
	{
		private readonly IAsyncDocumentSession session;
		private readonly static Guid SenderId = Guid.NewGuid();

		public DiscoveryModule(IAsyncDocumentSession session)
			: base("/api/discovery")
		{
			this.session = session;

			Get["/start"] = parameters =>
			{
				var discoveryClient = new ClusterDiscoveryClient(SenderId, "http://localhost:9020/api/discovery/notify");
				discoveryClient.PublishMyPresenceAsync();
				return "started";
			};

			Post["/notify", true] = async (parameters, ct) =>
			{
				var input = this.Bind<ServerRecord>("Id");

				var server = await session.Query<ServerRecord>().Where(s => s.Url == input.Url).FirstOrDefaultAsync() ?? new ServerRecord();
				this.BindTo(server, "Id");
				await session.StoreAsync(server);

				await HealthMonitorTask.FetchServerDatabases(server, session.Advanced.DocumentStore);

				return "notified";
			};
		}
	}
}