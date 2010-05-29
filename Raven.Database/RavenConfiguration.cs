using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Configuration;
using System.IO;
using log4net.Config;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Extensions;

namespace Raven.Database
{
	public class RavenConfiguration
	{
        public IDictionary<string, string> Settings { get; set; }

		public RavenConfiguration()
		{
		    Settings = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);

		    foreach (string setting in ConfigurationManager.AppSettings)
		    {
                if (setting.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
                    Settings[setting] = ConfigurationManager.AppSettings[setting];
		    }

			Catalog = new AggregateCatalog(
				new AssemblyCatalog(typeof (DocumentDatabase).Assembly)
				);

			Catalog.Changed += (sender, args) => ResetContainer();

			var portStr = ConfigurationManager.AppSettings["Raven/Port"];

			Port = portStr != null ? int.Parse(portStr) : 8080;

			var indexBatchSizeStr = ConfigurationManager.AppSettings["Raven/IndexingBatchSize"];

			IndexingBatchSize = indexBatchSizeStr != null ? int.Parse(indexBatchSizeStr) : 1024;

            var maxPageSizeStr = ConfigurationManager.AppSettings["Raven/MaxPageSize"];

            MaxPageSize = maxPageSizeStr != null ? int.Parse(maxPageSizeStr) : 1024;

			DataDirectory = ConfigurationManager.AppSettings["Raven/DataDir"] ?? @"~\Data";

			if (DataDirectory.StartsWith(@"~\"))
				DataDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, DataDirectory.Substring(2));

			WebDir = ConfigurationManager.AppSettings["Raven/WebDir"] ?? GetDefaultWebDir();

			var transactionMode = ConfigurationManager.AppSettings["Raven/TransactionMode"];
			TransactionMode result;
			if(Enum.TryParse(transactionMode, true, out result) == false)
				result = TransactionMode.Safe;
			TransactionMode = result;

			VirtualDirectory = ConfigurationManager.AppSettings["Raven/VirtualDirectory"] ?? "/";

			if (VirtualDirectory.EndsWith("/") )
				VirtualDirectory = VirtualDirectory.Substring(0, VirtualDirectory.Length - 1); 
			if (VirtualDirectory.StartsWith("/") == false)
				VirtualDirectory = "/" + VirtualDirectory;

			PluginsDirectory = ConfigurationManager.AppSettings["Raven/PluginsDirectory"] ?? @"~\Plugins";
			if (PluginsDirectory.StartsWith(@"~\"))
				PluginsDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, PluginsDirectory.Substring(2));

			AnonymousUserAccessMode = GetAnonymousUserAccessMode();
		}

	    public string ServerUrl
	    {
	        get
	        {
	            return "http://" + Environment.MachineName + ":" + Port + VirtualDirectory;
	        }
	    }

		private string pluginsDirectory;
		public string PluginsDirectory
		{
			get { return pluginsDirectory; }
			set
			{
				ResetContainer();
				// remove old directory catalog
				foreach (var directoryCatalogToRemove in Catalog.Catalogs.OfType<DirectoryCatalog>().Where(c=>c.Path == pluginsDirectory).ToArray())
				{
					Catalog.Catalogs.Remove(directoryCatalogToRemove);
				}
				
				pluginsDirectory = value;

				// add new one
				if(Directory.Exists(pluginsDirectory))
				{
					Catalog.Catalogs.Add(new DirectoryCatalog(pluginsDirectory));
				}
			}
		}

		private void ResetContainer()
		{
			if (Container != null && containerExternallySet == false)
			{
				Container.Dispose();
				Container = null;
			}
		}

		private static AnonymousUserAccessMode GetAnonymousUserAccessMode()
		{
			if (string.IsNullOrEmpty(ConfigurationManager.AppSettings["Raven/AnonymousAccess"]) == false)
			{
                var val = Enum.Parse(typeof(AnonymousUserAccessMode), ConfigurationManager.AppSettings["Raven/AnonymousAccess"]);
				return (AnonymousUserAccessMode) val;
			}
			return AnonymousUserAccessMode.Get;
		}

		private static string GetDefaultWebDir()
		{
            return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"Raven/WebUI");
		}

		public TransactionMode TransactionMode { get; set; }

        private string dataDirectory;
        public string DataDirectory 
        {
            get { return dataDirectory; }
            set { dataDirectory = value.ToFullPath(); }
        }

		public int Port { get; set; }
		public string WebDir { get; set; }
		public int IndexingBatchSize { get; set; }
		public AnonymousUserAccessMode AnonymousUserAccessMode { get; set; }

		public string VirtualDirectory { get; set; }

		private bool containerExternallySet;
		private CompositionContainer container;
		public CompositionContainer Container
		{
			get { return container ?? (container = new CompositionContainer(Catalog)); }
			set
			{
				containerExternallySet = true;
				container = value;
			}
		}

		public AggregateCatalog Catalog { get; set; }

		public bool RunInUnreliableYetFastModeThatIsNotSuitableForProduction { get; set; }

	    public int MaxPageSize { get; set; }

	    public void LoadLoggingSettings()
		{
			XmlConfigurator.ConfigureAndWatch(
				new FileInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log4net.config")));
		}

		public string GetFullUrl(string baseUrl)
		{
			if (baseUrl.StartsWith("/"))
				baseUrl = baseUrl.Substring(1);
            if (VirtualDirectory.EndsWith("/"))
                return VirtualDirectory + baseUrl;
			return VirtualDirectory + "/" + baseUrl;
		}

	    public T? GetConfigurationValue<T>(string configName) where T : struct
	    {
	        string value;
            // explicitly fail if we can convert it
            if (Settings.TryGetValue(configName, out value))
                return (T)Convert.ChangeType(value, typeof (T));
	        return null;
	    }
	}
}