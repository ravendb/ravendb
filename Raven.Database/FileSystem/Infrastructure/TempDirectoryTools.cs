using System.IO;

using Raven.Database.Config;

namespace Raven.Database.FileSystem.Infrastructure
{
	public class TempDirectoryTools
	{
		public static string Create(InMemoryRavenConfiguration configuration)
		{
			string tempDirectory;
			do
			{
				tempDirectory = Path.Combine(configuration.TempPath, Path.GetRandomFileName());
			} while (Directory.Exists(tempDirectory));
			Directory.CreateDirectory(tempDirectory);
			return tempDirectory;
		}
	}
}