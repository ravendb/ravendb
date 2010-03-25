using System.IO;
using System.Net;

namespace Raven.Server.Responders
{
	public class RavenUI : RequestResponder
	{
		public string RavenPath
		{
			get { return Settings.WebDir; }
		}

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
			var filePath = Path.Combine(RavenPath, docPath);
			if (File.Exists(filePath) == false)
			{
				context.SetStatusToNotFound();
				return;
			}
			var bytes = File.ReadAllBytes(filePath);
			context.Response.OutputStream.Write(bytes, 0, bytes.Length);
			context.Response.OutputStream.Flush();
		}
	}
}