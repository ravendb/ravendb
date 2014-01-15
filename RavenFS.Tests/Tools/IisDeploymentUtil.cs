using System;
using System.IO;
using Raven.Database.Server.RavenFS.Extensions;

namespace RavenFS.Tests.Tools
{
	public static class IisDeploymentUtil
	{
		private const string WebDirectoryTemplate = @".\RavenIISTestWeb_{0}\";

		public static string DeployWebProjectToTestDirectory(int port)
		{
		    var webDirectory = String.Format(WebDirectoryTemplate, port);
			var fullPath = Path.GetFullPath(webDirectory);
			if (Directory.Exists(fullPath))
				IOExtensions.DeleteDirectory(fullPath);

			IOExtensions.CopyDirectory(GetRavenWebSource(), webDirectory, new[]{"Data.ravenfs", "Index.ravenfs"});

			IOExtensions.DeleteDirectory(Path.Combine(fullPath, "Data.ravenfs"));
			IOExtensions.DeleteDirectory(Path.Combine(fullPath, "Index.ravenfs"));

			return fullPath;
		}

		private static string GetRavenWebSource()
		{
			foreach (var path in new[] { @"../../../RavenFS", "../RavenFS" })
			{
				var fullPath = Path.GetFullPath(path);

				if (Directory.Exists(fullPath) && File.Exists(Path.Combine(fullPath, "web.config")))
				{
					var combine = Path.Combine(fullPath, "bin");
					if (!Directory.Exists(combine) || Directory.GetFiles(combine, "RavenFS.dll").Length == 0)
						throw new Exception("RavenFS\\bin at " + fullPath + " was nonexistant or empty, you need to build RavenFS.");

					return fullPath;
				}
			}

			throw new FileNotFoundException("Could not find source directory for RavenFS");
		}
	}
}