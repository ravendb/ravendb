//-----------------------------------------------------------------------
// <copyright file="ForwardToRavenResponders.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Web;
using Raven.Database.Server;
using Raven.Http;
using Raven.Http.Abstractions;

namespace Raven.Web
{
	public class ForwardToRavenResponders : IHttpHandler
	{
		private readonly HttpServer server;

		public ForwardToRavenResponders(HttpServer server)
		{
			this.server = server;
		}

		public void ProcessRequest(HttpContext context)
		{
			server.HandleActualRequest(new HttpContextAdapter(context, server.Configuration));
		}

		public bool IsReusable
		{
			get { return true; }
		}
	}
}
