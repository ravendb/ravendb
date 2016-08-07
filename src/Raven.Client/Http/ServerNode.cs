using System;

namespace Raven.Client.Http
{
    public class ServerNode
    {
        public string Url;
        public string Database;
        public string ApiKey;
        public DateTime LastFailure;

        public bool Match(string url, string database)
        {
            return Url.Equals(url, StringComparison.OrdinalIgnoreCase) &&
                   Database.Equals(database, StringComparison.OrdinalIgnoreCase);
        }
    }
}