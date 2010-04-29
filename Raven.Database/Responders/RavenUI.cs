using Raven.Database.Abstractions;

namespace Raven.Database.Responders
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
			var docPath = context.Request.Url.LocalPath.Replace("/raven/", "");
			context.WriteEmbeddedFile(Settings.WebDir, docPath);
		}

		
	}
}