using System.Net;
using Raven.Server.Abstractions;

namespace Raven.Server.Responders
{
	public class RavenRoot : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/raven$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			context.Response.Redirect("/raven/index.html");
		}
	}
}