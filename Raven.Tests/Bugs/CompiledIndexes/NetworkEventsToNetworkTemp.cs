using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Database.Linq;

namespace Raven.Tests.Bugs.CompiledIndexes
{
	[CLSCompliant(false)]
	[DisplayName("Aggregates/NetworkTest")]
	public class NetworkEventsToNetworkTemp : AbstractViewGenerator
	{
		public NetworkEventsToNetworkTemp()
		{
			AddMapDefinition(docs => docs.Where(doc => doc["@metadata"]["Raven-Entity-Name"].ToString().Contains("NetworkList")));

			GroupByExtraction = source => source.Network;
			ReduceDefinition = Reduce;
			Indexes.Add("Network", FieldIndexing.NotAnalyzed);
			Indexes.Add("NetworkTimeStamp", FieldIndexing.NotAnalyzed);

			AddField("Network");
			AddField("NetworkTimeStamp");
			AddField("NetworkDetail");
		}

		private static IEnumerable<NetworkList> Reduce(IEnumerable<dynamic> source)
		{
			foreach (var events in source.GroupBy(@event => @event.Network))
			{
				foreach (var time in events.Select(x => new { NetworkTimeStamp =x.NetworkTimeStamp }).Distinct())
				{
					DateTime eventTime = new DateTime(2011,5,29).ToUniversalTime();
					yield return new NetworkList
					{
						Network = events.Key.ToString(),
						NetworkTimeStamp = eventTime,
					};
				}

			}
		}
	}
}