using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using Mono.CSharp;
using Raven.Abstractions.Extensions;
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
            Counters.ForEach(counter => countersArray.Add(counter.GetRavenJObject()));
            repMessage["Counters"] = countersArray;
            return repMessage;
        }

        public static ReplicationMessage GetReplicationMessage(RavenJObject jsonObject)
        {
			ReplicationMessage newReplicationMessage = new ReplicationMessage();
			newReplicationMessage.SendingServerName = jsonObject.Value<string>("SendingServerName");

            RavenJArray counters = jsonObject.Value<RavenJArray>("Counters");
			List<ReplicationCounter> countersList = new List<ReplicationCounter>();
			counters.ForEach(counter => countersList.Add(ReplicationCounter.GetReplicationCounter(counter)));
			newReplicationMessage.Counters = countersList;

			return newReplicationMessage;			
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
			var replicationCounterJObject = new RavenJObject();
			replicationCounterJObject["CounterName"] = CounterName;
			replicationCounterJObject["Etag"] = Etag;

            var serverValuesArray = new RavenJArray();
            ServerValues.ForEach(serverValue => serverValuesArray.Add(serverValue.GetRavenJObject()));
			replicationCounterJObject["ServerValues"] = serverValuesArray;

			return replicationCounterJObject;
        }

		public static ReplicationCounter GetReplicationCounter(RavenJToken jsonObject)
		{
			ReplicationCounter newReplicationCounter = new ReplicationCounter();
			newReplicationCounter.CounterName = jsonObject.Value<string>("CounterName");
			newReplicationCounter.Etag = jsonObject.Value<long>("Etag");

			RavenJArray serverValues = jsonObject.Value<RavenJArray>("ServerValues");
			List<PerServerValue> serverValuesList = new List<PerServerValue>();
			serverValues.ForEach(serverValue => serverValuesList.Add(PerServerValue.GetPerServerValue(serverValue)));
			newReplicationCounter.ServerValues = serverValuesList;

			return newReplicationCounter;
		}

        public class PerServerValue
        {
            public string ServerName { get; set; }
            public long Positive { get; set; }
            public long Negative { get; set; }

            public RavenJObject GetRavenJObject()
            {
				var replicationPerServerValue = new RavenJObject();
				replicationPerServerValue["ServerName"] = ServerName;
				replicationPerServerValue["Positive"] = Positive;
				replicationPerServerValue["Negative"] = Negative;
				return replicationPerServerValue;
            }

			public static PerServerValue GetPerServerValue(RavenJToken jsonObject)
			{
				PerServerValue newPerServerValue = new PerServerValue();
				newPerServerValue.ServerName = jsonObject.Value<string>("ServerName");
				newPerServerValue.Positive = jsonObject.Value<long>("Positive");
				newPerServerValue.Negative = jsonObject.Value<long>("Negative");

				return newPerServerValue;
			}
        }
    }
}