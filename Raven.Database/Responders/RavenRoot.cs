using Raven.Database.Abstractions;

namespace Raven.Database.Responders
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