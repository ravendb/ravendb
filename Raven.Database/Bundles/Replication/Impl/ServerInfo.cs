using System.Collections.Generic;

namespace Raven.Database.Bundles.Replication.Impl
{
    public class ServerInfo
    {
        public ServerInfo()
        {
            SourcesToIgnore = new List<string>();
        }

        public string SourceUrl { get; set; }

        public string DestinationUrl { get; set; }

        public string DatabaseName { get; set; }

        public string SourceId { get; set; }

        public List<string> SourcesToIgnore { get; set; }
    }
}