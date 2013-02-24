using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	class Plugins : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/plugins/status$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var dir = SystemDatabase.Configuration.PluginsDirectory;
			if (Directory.Exists(dir) == false)
			{
				context.WriteJson(new PluginsStatus());
				return;
			}

			var plugins = new PluginsStatus { Plugins = Directory.GetFiles(dir,"*.dll").Select(Path.GetFileNameWithoutExtension).ToList() };
			

			context.WriteJson(plugins);
		}
	}
}
