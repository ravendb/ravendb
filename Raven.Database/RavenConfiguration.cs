using System;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Primitives;
using System.Configuration;
using System.IO;
using log4net.Config;
using System.Linq;

namespace Raven.Database
{
	public class RavenConfiguration
	{
		public RavenConfiguration()
		{
			Catalog = new AggregateCatalog(
				new AssemblyCatalog(typeof(DocumentDatabase).Assembly)
				);

			Catalog.Changed += (sender, args) => ResetContainer();

			var portStr = ConfigurationManager.AppSettings["RavenPort"];

			Port = portStr != null ? int.Parse(portStr) : 8080;

			var indexBatchSizeStr = ConfigurationManager.AppSettings["IndexingBatchSize"];

			IndexingBatchSize = indexBatchSizeStr != null ? int.Parse(indexBatchSizeStr) : 1024;

			DataDirectory = ConfigurationManager.AppSettings["RavenDataDir"] ?? @"..\..\..\Data";

			WebDir = ConfigurationManager.AppSettings["RavenWebDir"] ?? GetDefaultWebDir();

			VirtualDirectory = ConfigurationManager.AppSettings["VirtualDirectory"];

			PluginsDirectory = ConfigurationManager.AppSettings["PluginsDirectory"] ?? "Plugins";

			AnonymousUserAccessMode = GetAnonymousUserAccessMode();

			ShouldCreateDefaultsWhenBuildingNewDatabaseFromScratch = true;
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
			if (string.IsNullOrEmpty(ConfigurationManager.AppSettings["AnonymousAccess"]) == false)
			{
				var val = Enum.Parse(typeof (AnonymousUserAccessMode), ConfigurationManager.AppSettings["AnonymousAccess"]);
				return (AnonymousUserAccessMode) val;
			}
			return AnonymousUserAccessMode.Get;
		}

		private static string GetDefaultWebDir()
		{
			return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\WebUI");
		}

		public string DataDirectory { get; set; }
		public int Port { get; set; }
		public string WebDir { get; set; }
		public int IndexingBatchSize { get; set; }
		public AnonymousUserAccessMode AnonymousUserAccessMode { get; set; }

		public bool ShouldCreateDefaultsWhenBuildingNewDatabaseFromScratch { get; set; }

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

		public void LoadLoggingSettings()
		{
			XmlConfigurator.ConfigureAndWatch(
				new FileInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log4net.config")));
		}

		public void RaiseDatabaseCreatedFromScratch(DocumentDatabase documentDatabase)
		{
			var onDatabaseCreatedFromScratch = DatabaseCreatedFromScratch;
			if (onDatabaseCreatedFromScratch != null)
				onDatabaseCreatedFromScratch(documentDatabase);
		}

		public event Action<DocumentDatabase> DatabaseCreatedFromScratch;
		
	}
}