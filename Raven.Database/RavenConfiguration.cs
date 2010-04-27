using System;
using System.Configuration;
using System.IO;
using log4net.Config;

namespace Raven.Database
{
	public class RavenConfiguration
	{
		public RavenConfiguration()
		{
			var portStr = ConfigurationManager.AppSettings["RavenPort"];

			Port = portStr != null ? int.Parse(portStr) : 8080;

			var indexBatchSizeStr = ConfigurationManager.AppSettings["IndexingBatchSize"];

			IndexingBatchSize = indexBatchSizeStr != null ? int.Parse(indexBatchSizeStr) : 1024;

			DataDirectory = ConfigurationManager.AppSettings["RavenDataDir"] ?? @"..\..\..\Data";

			WebDir = ConfigurationManager.AppSettings["RavenWebDir"] ?? GetDefaultWebDir();

			VirtualDirectory = ConfigurationManager.AppSettings["VirtualDirectory"];


			AnonymousUserAccessMode = GetAnonymousUserAccessMode();

			ShouldCreateDefaultsWhenBuildingNewDatabaseFromScratch = true;
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