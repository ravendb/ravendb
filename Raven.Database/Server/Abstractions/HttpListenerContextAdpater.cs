using System;
using System.Net;
using System.Security.Principal;

namespace Raven.Database.Server.Abstractions
{
	public class HttpListenerContextAdpater : IHttpContext
	{
		private readonly HttpListenerContext ctx;
		private readonly RavenConfiguration configuration;

		public HttpListenerContextAdpater(HttpListenerContext ctx, RavenConfiguration configuration)
		{
			this.ctx = ctx;
			this.configuration = configuration;
			Request = new HttpListenerRequestAdapter(ctx.Request);
			Response = new HttpListenerResponseAdapter(ctx.Response);
		}

		public RavenConfiguration Configuration
		{
			get { return configuration; }
		}

		public IHttpRequest Request
		{
			get;
			set;
		}

		public IHttpResponse Response
		{
			get;
			set;
		}

		public IPrincipal User
		{
			get { return ctx.User; }
		}

		public void FinalizeResonse()
		{
			try
			{
				ctx.Response.OutputStream.Flush();
				ctx.Response.Close();
			}
			catch
			{
			}
		}
	}
}