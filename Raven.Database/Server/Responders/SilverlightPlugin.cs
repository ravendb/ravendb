using System.IO;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class SilverlightPlugin : AbstractRequestResponder
	{
		public override string UrlPattern { get { return @"^/silverlight/plugin/(.+)$"; } }

		public override string[] SupportedVerbs { get { return new[] {"GET"}; } }

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var pluginUrl = match.Groups[1].Value + ".xap";

			var dir = ResourceStore.Configuration.PluginsDirectory;
			var path = Path.Combine(dir, pluginUrl);

			if (File.Exists(path))
			{
				context.WriteFile(path);
			}
			else
			{
				context.SetStatusToNotFound();
			}
		}
	}
}