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

namespace Raven.ClusterManager.Modules
{
	public class DiscoveryModule : NancyModule
	{
		private readonly IDocumentSession session;
		private readonly static Guid senderId = Guid.NewGuid();
		private readonly ClusterDiscoveryClient discoveryClient;

		public DiscoveryModule(IDocumentSession session): base("/api/discovery")
		{
			this.session = session;
			discoveryClient = new ClusterDiscoveryClient(senderId, "http://localhost:9020/api/discovery/notify");

			Get["/start"] = parameters =>
			{
				discoveryClient.PublishMyPresence();
				return "started";
			};

			Post["/notify"] = parameters =>
			{
				var server = this.Bind<Server>("Id");
				session.Store(server);
				return "notified";
			};
		}
	}
}