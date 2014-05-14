using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using Mono.CSharp;
using Raven.Json.Linq;

namespace Raven.Database.Counters
{
    public class ReplicationMessage: NameValueCollection
    {
        public string SendingServerName { get; set; }
        public List<ReplicationCounter> Counters { get; set; }

        public RavenJObject GetRavenJObject()
        {
            var repMessage = new RavenJObject();
            repMessage["SendingServerName"] = SendingServerName;
            var countersArray = new RavenJArray();
            Counters.ForEach((ReplicationCounter counter) => countersArray.Add(counter.GetRavenJObject()));
            repMessage["Counters"] = countersArray;
//            repMessage["Counters"] = new RavenJArray()
            return repMessage;
        }


        public static ReplicationMessage GetReplicationMessage(RavenJObject jsonObject)
        {
            ReplicationMessage newRepMessage = new ReplicationMessage();
            newRepMessage.SendingServerName = jsonObject.Value<string>("SendingServerName");
            RavenJArray counters = jsonObject.Value<RavenJArray>("Counters");

            /*foreach (RavenJObject document in jsonObject)
            {
                var metadata = document.Value<RavenJObject>("@metadata");
                if (metadata[Constants.RavenReplicationSource] == null)
                {
                    // not sure why, old document from when the user didn't have replication
                    // that we suddenly decided to replicate, choose the source for that
                    metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(src);
                }
                lastEtag = metadata.Value<string>("@etag");
                var id = metadata.Value<string>("@id");
                document.Remove("@metadata");
                ReplicateDocument(actions, id, metadata, document, src);
            }*/

            return newRepMessage;
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

        public RavenJObject GetRavenJObject()
        {
            var repCounterJObject = new RavenJObject();
            repCounterJObject["CounterName"] = CounterName;
            repCounterJObject["Etag"] = Etag;

            var serverValuesArray = new RavenJArray();
//            serverValuesArray = new RavenJArray(ServerValues.Select(serverValue => serverValue.GetRavenJObject()).GetEnumerator());
            ServerValues.ForEach(serverValue => serverValuesArray.Add(serverValue.GetRavenJObject()));

            repCounterJObject["ServerValues"] = serverValuesArray;
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