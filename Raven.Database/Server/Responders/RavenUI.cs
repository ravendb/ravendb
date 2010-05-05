using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class RavenUI : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/raven/"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var docPath = context.GetRequestUrl().Replace("/raven/", "");
			context.WriteEmbeddedFile(Settings.WebDir, docPath);
		}

		
	}
}