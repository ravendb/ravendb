using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Counters
{
	public class CountersChangeTests : RavenBaseCountersTest
	{
		private const string CounterStorageName = "FooBarCounterStore";
		private const string CounterName = "FooBarCounter";

		[Theory]
		[InlineData(2)]
		[InlineData(-2)]
		public async Task CountrsReset_should_work(int delta)
		{
			using (var store = NewRemoteCountersStore())
			using (var client = store.NewCounterClient(CounterStorageName))
			{
				await store.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);

				const string CounterGroupName = "FooBarGroup";
				await client.Commands.ChangeAsync(CounterGroupName, CounterName,delta);

				var total = await client.Commands.GetOverallTotalAsync(CounterGroupName, CounterName);
				total.Should().Be(delta);

				await client.Commands.ResetAsync(CounterGroupName, CounterName);

				total = await client.Commands.GetOverallTotalAsync(CounterGroupName, CounterName);
				total.Should().Be(0);
			}			
		}

		[Fact]
		public async Task CountersIncrement_should_work()
		{
			using (var store = NewRemoteCountersStore())
			using (var client = store.NewCounterClient(CounterStorageName))
			{
				await store.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);

				const string CounterGroupName = "FooBarGroup12";
				await client.Commands.IncrementAsync(CounterGroupName, CounterName);

				var total = await client.Commands.GetOverallTotalAsync(CounterGroupName, CounterName);
				total.Should().Be(1);

				await client.Commands.IncrementAsync(CounterGroupName, CounterName);

				total = await client.Commands.GetOverallTotalAsync(CounterGroupName, CounterName);
				total.Should().Be(2);
			}
		}

		[Fact]
		public async Task Counters_change_should_work()
		{
			using (var store = NewRemoteCountersStore())
			using (var client = store.NewCounterClient(CounterStorageName))
			{
				await store.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);

				const string CounterGroupName = "FooBarGroup";
				await client.Commands.ChangeAsync(CounterGroupName, CounterName, 5);

				var total = await client.Commands.GetOverallTotalAsync(CounterGroupName, CounterName);
				total.Should().Be(5);

				await client.Commands.ChangeAsync(CounterGroupName,CounterName, -30);

				total = await client.Commands.GetOverallTotalAsync(CounterGroupName, CounterName);
				total.Should().Be(-25);
			}
		}
	}
}
