using System.IO;

namespace Raven.Database.Server.RavenFS.Infrastructure
{
	public class TempDirectoryTools
	{
		public static string Create()
		{
			string tempDirectory;
			do
			{
				tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
			} while (Directory.Exists(tempDirectory));
			Directory.CreateDirectory(tempDirectory);
			return tempDirectory;
		}
	}
}