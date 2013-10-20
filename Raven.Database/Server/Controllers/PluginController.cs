using System.IO;
using System.Linq;
using System.Net.Http;
using System.Web.Http;
using Raven.Database.Plugins;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class PluginController : RavenApiController
	{
		[HttpGet][Route("plugins/status")]
		public HttpResponseMessage PlugingsStatusGet()
		{
			var dir = DatabasesLandlord.SystemDatabase.Configuration.PluginsDirectory;
			if (Directory.Exists(dir) == false)
				return GetMessageWithObject(new PluginsStatus());

			var plugins = new PluginsStatus { Plugins = Directory.GetFiles(dir, "*.dll").Select(Path.GetFileNameWithoutExtension).ToList() };

			return GetMessageWithObject(plugins);
		}
	}
}