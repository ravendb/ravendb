using System;
using System.IO;
using System.Security.Principal;
using System.Web;

namespace Raven.Database.Server.Abstractions
{
	public class HttpContextAdapter : IHttpContext
	{
		private readonly HttpContext context;
		private readonly HttpRequestAdapter request;
		private readonly HttpResponseAdapter response;
		private readonly RavenConfiguration configuration;

		public HttpContextAdapter(HttpContext context, RavenConfiguration configuration)
		{
			this.context = context;
			this.configuration = configuration;
			request = new HttpRequestAdapter(context.Request);
			response = new HttpResponseAdapter(context.Response);
		}

		public RavenConfiguration Configuration
		{
			get { return configuration; }
		}

		public IHttpRequest Request
		{
			get { return request; }
		}

		public IHttpResponse Response
		{
			get { return response; }
		}

		public IPrincipal User
		{
			get { return context.User; }
		}

		public void FinalizeResonse()
		{
			
		}

		public void SetResponseFilter(Func<Stream, Stream> responseFilter)
		{
			context.Response.Filter = responseFilter(context.Response.Filter);
		}
	}
}