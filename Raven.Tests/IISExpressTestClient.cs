using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Xml;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Tests.Util;
using Xunit;

namespace Raven.Tests
{
	public class IisExpressTestClient : IDisposable
	{
		private const string WebDirectory = @".\RavenIISTestWeb\";
		public static int Port = 8084;
		private IISExpressDriver iisExpress;

		private static string DeployWebProjectToTestDirectory()
		{
			IOExtensions.DeleteDirectory(Path.GetFullPath(WebDirectory));

			IOExtensions.CopyDirectory(GetRavenWebSource(), WebDirectory);

			if (Directory.Exists(Path.Combine(WebDirectory, "Data")))
			{
				IOExtensions.DeleteDirectory(Path.Combine(WebDirectory, "Data"));
			}

			return Path.GetFullPath(WebDirectory);
		}

		private static string GetRavenWebSource()
		{
			foreach (var path in new[] { @".\..\..\..\Raven.Web", @".\_PublishedWebsites\Raven.Web" })
			{
				var fullPath = Path.GetFullPath(path);

				if (Directory.Exists(fullPath) && File.Exists(Path.Combine(fullPath, "web.config")))
				{
					var combine = Path.Combine(fullPath, "bin");
					if (!Directory.Exists(combine) || Directory.GetFiles(combine, "Raven.Web.dll").Length == 0)
						throw new Exception("Raven.Web\\bin at " + fullPath + " was nonexistent or empty, you need to build Raven.Web.");

					return fullPath;
				}
			}

			throw new FileNotFoundException("Could not find source directory for Raven.Web");
		}

		public IDocumentStore NewDocumentStore(bool fiddler = false, Dictionary<string, string> settings = null)
		{
			if (iisExpress == null)
			{
				iisExpress = new IISExpressDriver();
				var iisTestWebDirectory = DeployWebProjectToTestDirectory();

				if (settings != null)
				{
					ModifyWebConfig(Path.Combine(iisTestWebDirectory, "web.config"), settings);
				}

				iisExpress.Start(iisTestWebDirectory, 8084);
			}

			var url = iisExpress.Url;
			if (fiddler)
				url = url.Replace("localhost", "localhost.fiddler");
			return new DocumentStore {Url = url}.Initialize();
		}

		private void ModifyWebConfig(string webConfigPath, Dictionary<string, string> settings)
		{
			var webConfig = new XmlDocument();

			webConfig.Load(webConfigPath);

			var appSettingsNode = webConfig.SelectSingleNode("/configuration/appSettings");

			if (appSettingsNode == null)
				throw new InvalidOperationException("Web.config file does not contains <appSettings> section");

			foreach (var setting in settings)
			{
				var newElement = webConfig.CreateElement("add");
				newElement.SetAttribute("key", setting.Key);
				newElement.SetAttribute("value", setting.Value);
				appSettingsNode.AppendChild(newElement);
			}

			webConfig.Save(webConfigPath);
		}

		protected void WaitForIndexing(IDocumentStore store, string db = null)
		{
			var databaseCommands = store.DatabaseCommands;
			if (db != null)
				databaseCommands = databaseCommands.ForDatabase(db);
			Assert.True(SpinWait.SpinUntil(() => databaseCommands.GetStatistics().StaleIndexes.Length == 0, TimeSpan.FromMinutes(10)));
		}

		public void Dispose()
		{
			if (iisExpress != null)
			{
				iisExpress.Dispose();
				iisExpress = null;
			}

			IOExtensions.DeleteDirectory(Path.GetFullPath(WebDirectory));
		}
	}
}