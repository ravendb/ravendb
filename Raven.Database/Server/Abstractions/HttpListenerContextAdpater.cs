using System;
using System.IO;
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
			ResponseInternal = new HttpListenerResponseAdapter(ctx.Response);
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

		protected HttpListenerResponseAdapter ResponseInternal { get; set; }
		
		public IHttpResponse Response
		{
			get { return ResponseInternal; }
		}

		public IPrincipal User
		{
			get { return ctx.User; }
		}

		public void FinalizeResonse()
		{
			try
			{
				ResponseInternal.OutputStream.Flush();
				ResponseInternal.OutputStream.Dispose(); // this is required when using compressing stream
				ctx.Response.Close();
			}
			catch
			{
			}
		}

		public void SetResponseFilter(Func<Stream, Stream> responseFilter)
		{
			ResponseInternal.OutputStream = responseFilter(ResponseInternal.OutputStream);
		}
	}
}