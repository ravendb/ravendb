using System.IO;

namespace Raven.Client.Smuggler
{
    public class DatabaseSmugglerFileDestination : IDatabaseSmugglerDestination
    {
        public string FilePath;

        public Stream CreateStream()
        {
            var fileStream = File.Create(FilePath);
            return fileStream;
        }
    }
}