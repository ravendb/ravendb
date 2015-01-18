using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Client.Counters;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CountersBatchTests : BaseCountersTest
	{
		private const string CounterStorageName = "FooBarCounter";

		[Fact]
		public async Task CountersBatch_should_work()
		{
			using (var server = GetNewServer(port:9000))
			using (var store = NewRemoteDocumentStore(ravenDbServer:server, fiddler: true))
			using (var countersClient = store.NewCountersClient(CounterStorageName))
			{
				await countersClient.Admin.CreateCounterStorageAsync(new CountersDocument()
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				},CounterStorageName);
				using (var counterBatch = countersClient.NewBatch(new CountersBatchOptions{ BatchSizeLimit = 3 }))
				{
					counterBatch.Increment("FooGroup", CounterStorageName);
					counterBatch.Increment("FooGroup", CounterStorageName);
					counterBatch.Decrement("FooGroup", CounterStorageName);
				}

				var total = await countersClient.Commands.GetOverallTotalAsync("FooGroup", CounterStorageName);
				total.Should().Be(1);
			}
		}
	}
}
