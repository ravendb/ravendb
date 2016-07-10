using System.IO;

namespace Raven.Client.Smuggler
{
    public class DatabaseSmugglerRemoteDestination : IDatabaseSmugglerDestination
    {
        public string Url;
        public string Database;
        public Stream CreateStream()
        {
            throw new System.NotImplementedException();
        }
    }
}