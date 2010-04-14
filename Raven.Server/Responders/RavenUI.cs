using System.IO;
using System.Net;

namespace Raven.Server.Responders
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

		public override void Respond(HttpListenerContext context)
		{
			var docPath = context.Request.Url.LocalPath.Replace("/raven/", "");
			context.WriteEmbeddedFile(Settings.WebDir, docPath);
		}

		
	}
}