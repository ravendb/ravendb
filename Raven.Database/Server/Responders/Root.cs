using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Root : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "/$"; }
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