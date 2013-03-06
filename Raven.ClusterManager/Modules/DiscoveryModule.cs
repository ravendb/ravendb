// -----------------------------------------------------------------------
//  <copyright file="DiscoveryModule.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using Nancy;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Listeners;
using Raven.ClusterManager.Discovery;
using Raven.ClusterManager.Models;
using Nancy.ModelBinding;
using System.Linq;
using Raven.ClusterManager.Tasks;

namespace Raven.ClusterManager.Modules
{
	public class DiscoveryModule : NancyModule
	{
		private readonly IDocumentSession session;
		private readonly static Guid SenderId = Guid.NewGuid();

		public DiscoveryModule(IDocumentSession session)
			: base("/api/discovery")
		{
			this.session = session;

			Get["/start"] = parameters =>
			{
				var discoveryClient = new ClusterDiscoveryClient(SenderId, "http://localhost:9020/api/discovery/notify");
				discoveryClient.PublishMyPresenceAsync();
				return "started";
			};

			Post["/notify"] = parameters =>
			{
				var input = this.Bind<ServerRecord>("Id");

				var server = session.Query<ServerRecord>().FirstOrDefault(s => s.Url == input.Url) ?? new ServerRecord();
				this.BindTo(server, "Id");
				session.Store(server);

				HealthMonitorTask.FetchServerDatabases(server, session);

				return "notified";
			};

			Get["/servers"] = parameters =>
			{
				var servers = this.session.Query<ServerRecord>().ToList();
				return new ClusterStatistics
				{
					Servers = servers,
				};
			};
		}
	}
}