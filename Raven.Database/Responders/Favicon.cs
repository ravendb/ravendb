using Raven.Database.Abstractions;

namespace Raven.Database.Responders
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