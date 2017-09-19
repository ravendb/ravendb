using System;
using Raven.Client.Util;

namespace Raven.Server.ServerWide
{
    public class ServerStatistics
    {
        public ServerStatistics()
        {
            StartUpTime = SystemTime.UtcNow;
        }

        public readonly DateTime StartUpTime;
        
        public DateTime? LastRequestTime { get; set; }
    }
}
