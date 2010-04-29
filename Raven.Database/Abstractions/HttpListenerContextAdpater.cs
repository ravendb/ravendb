using System.Net;
using System.Security.Principal;

namespace Raven.Database.Abstractions
{
	public class HttpListenerContextAdpater : IHttpContext
	{
		private readonly HttpListenerContext ctx;

		public HttpListenerContextAdpater(HttpListenerContext ctx)
		{
			this.ctx = ctx;
			Request = new HttpListenerRequestAdapter(ctx.Request);
			Response = new HttpListenerResponseAdapter(ctx.Response);
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
	}
}