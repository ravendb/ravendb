using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CountersChangeTests : RavenBaseCountersTest
	{
		private const string CounterStorageName = "FooBarCounter";

		[Fact]
		public async Task CountersIncrement_should_work()
		{
			using (var store = NewRemoteCountersStore())
			using (var client = store.NewCounterClient(CounterStorageName))
			{
				await store.CreateCounterAsync(new CountersDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);

				const string CounterGroupName = "FooBarGroup";
				await client.Commands.IncrementAsync(CounterGroupName);

				var total = await client.Commands.GetOverallTotalAsync(CounterGroupName);
				total.Should().Be(1);

				await client.Commands.IncrementAsync(CounterGroupName);

				total = await client.Commands.GetOverallTotalAsync(CounterGroupName);
				total.Should().Be(2);
			}
		}

		[Fact]
		public async Task Counters_change_should_work()
		{
			using (var store = NewRemoteCountersStore())
			using (var client = store.NewCounterClient(CounterStorageName))
			{
				await store.CreateCounterAsync(new CountersDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);

				const string CounterGroupName = "FooBarGroup";
				await client.Commands.ChangeAsync(CounterGroupName,5);

				var total = await client.Commands.GetOverallTotalAsync(CounterGroupName);
				total.Should().Be(5);

				await client.Commands.ChangeAsync(CounterGroupName, -30);

				total = await client.Commands.GetOverallTotalAsync(CounterGroupName);
				total.Should().Be(-25);
			}
			
		}
	}
}
