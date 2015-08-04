using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Util;
using Xunit;
using Xunit.Extensions;
using Xunit.Sdk;

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
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);

				const string counterGroupName = "FooBarGroup";
				await store.ChangeAsync(counterGroupName, CounterName, delta);

				var total = await store.GetOverallTotalAsync(counterGroupName, CounterName);
				total.Should().Be(delta);
				await store.ResetAsync(counterGroupName, CounterName);

				total = await store.GetOverallTotalAsync(counterGroupName, CounterName);
				total.Should().Be(0);
			}	
		}

		[Theory]
		[InlineData(2)]
		[InlineData(-2)]
		public async Task CountersDelete_should_work(int delta)
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);

				const string counterGroupName = "FooBarGroup";
				await store.ChangeAsync(counterGroupName, CounterName, delta);

				var total = await store.GetOverallTotalAsync(counterGroupName, CounterName);
				total.Should().Be(delta);

				store.Invoking(x => AsyncHelpers.RunSync(() => x.DeleteAsync(counterGroupName, CounterName)))
					 .ShouldNotThrow<InvalidOperationException>();
			}
		}

		[Fact]
		public async Task CountersIncrement_should_work()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);

				const string CounterGroupName = "FooBarGroup12";
				await store.IncrementAsync(CounterGroupName, CounterName);

				var total = await store.GetOverallTotalAsync(CounterGroupName, CounterName);
				total.Should().Be(1);

				await store.IncrementAsync(CounterGroupName, CounterName);

				total = await store.GetOverallTotalAsync(CounterGroupName, CounterName);
				total.Should().Be(2);
			}
		}

		[Fact]
		public async Task Counters_change_should_work()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);

				const string CounterGroupName = "FooBarGroup";
				await store.ChangeAsync(CounterGroupName, CounterName, 5);

				var total = await store.GetOverallTotalAsync(CounterGroupName, CounterName);
				total.Should().Be(5);

				await store.ChangeAsync(CounterGroupName, CounterName, -30);

				total = await store.GetOverallTotalAsync(CounterGroupName, CounterName);
				total.Should().Be(-25);
			}
		}
	}
}
