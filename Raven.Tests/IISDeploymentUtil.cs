using System;
using System.IO;
using Raven.Client;
using Raven.Database.Extensions;

namespace Raven.Tests
{
	public class IISDeploymentUtil
	{
		protected const string WebDirectory = @".\RavenIISTestWeb\";

		public static string DeployWebProjectToTestDirectory()
		{
			var fullPath = Path.GetFullPath(WebDirectory);
			if (Directory.Exists(fullPath))
			{
				IOExtensions.DeleteDirectory(fullPath);
			}

			IOExtensions.CopyDirectory(GetRavenWebSource(), WebDirectory);

			return fullPath;
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
						throw new Exception("Raven.Web\\bin at " + fullPath + " was nonexistant or empty, you need to build Raven.Web.");

					return fullPath;
				}
			}

			throw new FileNotFoundException("Could not find source directory for Raven.Web");
		}
	}
}