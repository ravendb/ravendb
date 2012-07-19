// -----------------------------------------------------------------------
//  <copyright file="SiganlRCurrentDatabaseForwardingHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using System.Web;
using Raven.Abstractions.Util;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Imports.SignalR.Hosting.AspNet;

namespace Raven.Web
{
	public class SiganlRCurrentDatabaseForwardingHandler : HttpTaskAsyncHandler
	{
		private readonly HttpServer server;
		private readonly HttpTaskAsyncHandler handler;

		public SiganlRCurrentDatabaseForwardingHandler(HttpServer server, HttpTaskAsyncHandler handler)
		{
			this.server = server;
			this.handler = handler;
		}

		public override Task ProcessRequestAsync(HttpContextBase context)
		{
			var httpContextAdapter = new HttpContextAdapter(HttpContext.Current, server.Configuration);
			return server.HandleSignalRequest(httpContextAdapter, 
				prefix => handler.ProcessRequestAsync(context),
				() => new CompletedTask());
		}
	}
}