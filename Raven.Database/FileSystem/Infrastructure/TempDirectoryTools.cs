using System.IO;

using Raven.Database.Config;

namespace Raven.Database.FileSystem.Infrastructure
{
    public class TempDirectoryTools
    {
        public static string Create(RavenConfiguration configuration)
        {
            string tempDirectory;
            do
            {
                tempDirectory = Path.Combine(configuration.Core.TempPath, Path.GetRandomFileName());
            } while (Directory.Exists(tempDirectory));
            Directory.CreateDirectory(tempDirectory);
            return tempDirectory;
        }
    }
}
