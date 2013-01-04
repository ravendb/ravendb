using System;
using System.IO;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Tests.Util;

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

		public IDocumentStore NewDocumentStore()
		{
			if (iisExpress == null)
			{
				iisExpress = new IISExpressDriver();
				iisExpress.Start(DeployWebProjectToTestDirectory(), 8084);
			}

			return new DocumentStore {Url = iisExpress.Url}.Initialize();
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