using System.Security.Principal;
using System.Web;

namespace Raven.Database.Server.Abstractions
{
	public class HttpContextAdapter : IHttpContext
	{
		private readonly HttpContext context;
		private readonly HttpRequestAdapter request;
		private readonly HttpResponseAdapter response;

		public HttpContextAdapter(HttpContext context)
		{
			this.context = context;
			request = new HttpRequestAdapter(context.Request);
			response = new HttpResponseAdapter(context.Response);
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
			// here it is a no op
		}
	}
}