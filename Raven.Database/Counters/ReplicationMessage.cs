using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using Mono.CSharp;
using Newtonsoft.Json;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Counters
{
    public class ReplicationMessage
    {
        public string SendingServerName { get; set; }
        public List<ReplicationCounter> Counters { get; set; }


        public ReplicationMessage()
        {
            Counters = new List<ReplicationCounter>();
        }

        public RavenJObject GetRavenJObject()
        {
            return RavenJObject.FromObject(this);
        }

        public static ReplicationMessage GetReplicationMessage(RavenJObject obj)
        {
            return obj.JsonDeserialization<ReplicationMessage>();
        }

        
    }

    public class ReplicationCounter
    {
        public string CounterName { get; set; }
        public long Etag;
        public List<PerServerValue> ServerValues { get; set; }

        public ReplicationCounter()
        {
            ServerValues = new List<PerServerValue>();
        }

        public class PerServerValue
        {
            public string ServerName { get; set; }
            public long Positive { get; set; }
            public long Negative { get; set; }
        }
    }
}