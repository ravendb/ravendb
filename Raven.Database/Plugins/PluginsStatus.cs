using System.Collections.Generic;

namespace Raven.Database.Plugins
{
	public class PluginsStatus
	{
		public List<string> Plugins { get; set; }

		public PluginsStatus()
		{
			Plugins = new List<string>();
		}
	}
}
