using System.Collections.Generic;
using System.Linq;
using Mono.CSharp;
using Raven.Json.Linq;

namespace Raven.Database.Counters
{
    public class ReplicationMessage
    {
        public string SendingServerName { get; set; }
        public List<ReplicationCounter> Counters { get; set; }

        public RavenJObject GetRavenJObject()
        {
            var repMessage = new RavenJObject();
            repMessage["SendingServerName"] = SendingServerName;
            repMessage["Counters"] = new RavenJArray(Counters.Select(counter => counter.GetRavenJObject()).GetEnumerator());

            return repMessage;
        }
    }

    public class ReplicationCounter
    {
        public RavenJObject Json;
        
        public string CounterName { get; set; }
        public long Etag;
        public List<PerServerValue> ServerValues { get; set; }

        public ReplicationCounter()
        {
            ServerValues = new List<PerServerValue>();
        }

        public RavenJObject GetRavenJObject()
        {
            var repCounterJObject = new RavenJObject();
            repCounterJObject["CounterName"] = CounterName;
            repCounterJObject["Etag"] = Etag;
            repCounterJObject["ServerValues"] = new RavenJArray(ServerValues.Select(serverValue => serverValue.GetRavenJObject()).GetEnumerator());

            return repCounterJObject;
        }

        public class PerServerValue
        {
            public string ServerName { get; set; }
            public long Positive { get; set; }
            public long Negative { get; set; }

            public RavenJObject GetRavenJObject()
            {
                var repPerServerValue = new RavenJObject();
                repPerServerValue["ServerName"] = ServerName;
                repPerServerValue["Positive"] = Positive;
                repPerServerValue["Negative"] = Negative;
                return repPerServerValue;
            }
        }
    }
}