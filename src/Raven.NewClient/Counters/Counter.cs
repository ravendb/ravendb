using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.NewClient.Abstractions.Counters
{
    public class Counter
    {
        public Counter()
        {
            ServerValues = new List<ServerValue>();
        }

        public List<ServerValue> ServerValues { get; private set; }

        public Guid LocalServerId { get; set; }

        public Guid LastUpdateByServer { get; set; }

        public long Total
        {
            get { return ServerValues.Sum(x => x.Value); }
        }

        public long NumOfServers
        {
            get { return ServerValues.Select(x => x.ServerId).Distinct().Count(); }
        }
    }

    public class ServerValue
    {
        public Guid ServerId { get; set; }

        public long Value { get; set; }

        public string ServerName { get; set; }

        public long Etag { get; set; }
    }
}
