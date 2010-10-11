using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Favicon : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/favicon.ico$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			context.WriteEmbeddedFile(Settings.WebDir,"favicon.ico");
		}
	}
}
