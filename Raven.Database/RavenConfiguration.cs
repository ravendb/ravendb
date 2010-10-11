using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Configuration;
using System.IO;
using log4net.Config;
using System.Linq;
using Raven.Database.Extensions;
using Raven.Database.Storage;

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

			HostName = ConfigurationManager.AppSettings["Raven/HostName"];

			var portStr = ConfigurationManager.AppSettings["Raven/Port"];

			Port = portStr != null ? int.Parse(portStr) : 8080;

            var maxPageSizeStr = ConfigurationManager.AppSettings["Raven/MaxPageSize"];
            var minimumQueryCount = ConfigurationManager.AppSettings["Raven/TempIndexPromotionMinimumQueryCount"];
            var queryThreshold = ConfigurationManager.AppSettings["Raven/TempIndexPromotionThreshold"];
            var cleanupPeriod = ConfigurationManager.AppSettings["Raven/TempIndexCleanupPeriod"];
            var cleanupThreshold = ConfigurationManager.AppSettings["Raven/TempIndexCleanupThreshold"];

            MaxPageSize = maxPageSizeStr != null ? int.Parse(maxPageSizeStr) : 1024;
            TempIndexPromotionMinimumQueryCount = minimumQueryCount != null ? int.Parse(minimumQueryCount) : 100;
            TempIndexPromotionThreshold = queryThreshold != null ? int.Parse(queryThreshold) : 60000;   // once a minute
            TempIndexCleanupPeriod = cleanupPeriod != null ? int.Parse(cleanupPeriod) : 300;            // every 5 minutes
            TempIndexCleanupThreshold = cleanupThreshold != null ? int.Parse(cleanupThreshold) : 600;   // 10 minutes inactivity

			DataDirectory = ConfigurationManager.AppSettings["Raven/DataDir"] ?? @"~\Data";

			WebDir = ConfigurationManager.AppSettings["Raven/WebDir"] ?? GetDefaultWebDir();

		    AccessControlAllowOrigin = ConfigurationManager.AppSettings["Raven/AccessControlAllowOrigin"];

			bool httpCompressionTemp;
			if (bool.TryParse(ConfigurationManager.AppSettings["Raven/HttpCompression"], out httpCompressionTemp) == false)
				httpCompressionTemp = true;
			HttpCompression = httpCompressionTemp;

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

            //StorageTypeName = ConfigurationManager.AppSettings["Raven/StorageTypeName"] ??
            //    "Raven.Storage.Esent.TransactionalStorage, Raven.Storage.Esent";

            StorageTypeName = ConfigurationManager.AppSettings["Raven/StorageTypeName"] ??
                "Raven.Storage.Managed.TransactionalStorage, Raven.Storage.Managed";
		}

		public string StorageTypeName { get; set; }

		public string ServerUrl
	    {
	        get
	        {
	            return "http://" + (HostName ?? Environment.MachineName) + ":" + Port + VirtualDirectory;
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

        /// <summary>
        /// null to accept any hostname or address
        /// </summary>
        public string HostName { get; set;  } 
		public int Port { get; set; }
		public string WebDir { get; set; }
        public string AccessControlAllowOrigin { get; set; }
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

		public bool HttpCompression { get; set; }

	    public int MaxPageSize { get; set; }


        public int TempIndexPromotionThreshold { get; set; }
        public int TempIndexPromotionMinimumQueryCount { get; set; }
        public int TempIndexCleanupPeriod { get; set; }
        public int TempIndexCleanupThreshold { get; set; }

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

		public ITransactionalStorage CreateTransactionalStorage(Action notifyAboutWork)
		{
			var type = 
				Type.GetType(StorageTypeName.Split(',').First()) ?? // first try to find the merged one
				Type.GetType(StorageTypeName); // then try full type name

			if(type == null)
				throw new InvalidOperationException("Could not find transactional storage type: " + StorageTypeName);

			Catalog.Catalogs.Add(new AssemblyCatalog(type.Assembly));

			return (ITransactionalStorage)Activator.CreateInstance(type, this, notifyAboutWork);
		}

	}
}
