using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using Raven.Database;

namespace Raven.Server.Responders
{
	public abstract class RequestResponder
	{
		private readonly string[] supportedVerbsCached;
		protected readonly Regex urlMatcher;

		protected RequestResponder()
		{
			urlMatcher = new Regex(UrlPattern);
			supportedVerbsCached = SupportedVerbs;
		}

		public abstract string UrlPattern { get; }
		public abstract string[] SupportedVerbs { get; }

		public DocumentDatabase Database { get; set; }
		public RavenConfiguration Settings { get; set; }

		public bool WillRespond(HttpListenerContext context)
		{
			var match = urlMatcher.Match(context.Request.Url.LocalPath);
			return match.Success && supportedVerbsCached.Contains(context.Request.HttpMethod);
		}


		public abstract void Respond(HttpListenerContext context);
	}
}