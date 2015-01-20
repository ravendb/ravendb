using System;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Xunit;

namespace Raven.Tests.Counters
{
	public class AdminClientTests : RavenBaseCountersTest
	{
		private const string CounterName = "FooBarCounter";

		[Fact]
		public void Should_be_able_to_initialize()
		{
			Assert.DoesNotThrow(() =>
			{
				using (var counterStore = NewRemoteCountersStore())
				using (var client = counterStore.NewCounterClient(CounterName))
				{
				}
			});
		}

		[Fact]
		public async Task Should_be_able_to_create_counter()
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterAsync(new CountersDocument(), CounterName);

				var counterStorageNames = await store.GetCounterStoragesNamesAsync();
				counterStorageNames.Should().HaveCount(1)
					.And.Contain(CounterName);
			}
		}

		[Fact]
		public async Task Should_be_able_to_create_multiple_counters()
		{
			var expectedClientNames = new[] { CounterName + "A", CounterName + "B", CounterName + "C" };
			using (var store = NewRemoteCountersStore())
			{
				var defaultCountersDocument = new CountersDocument();
				await store.CreateCounterAsync(defaultCountersDocument, expectedClientNames[0]);
				await store.CreateCounterAsync(defaultCountersDocument, expectedClientNames[1]);
				await store.CreateCounterAsync(defaultCountersDocument, expectedClientNames[2]);

				var counterStorageNames = await store.GetCounterStoragesNamesAsync();
				counterStorageNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}

		[Fact]
		public async Task Should_be_able_to_create_multiple_counters_in_parallel()
		{
			var expectedClientNames = new[] { CounterName + "A", CounterName + "B", CounterName + "C" };
			using (var store = NewRemoteCountersStore())
			{
				var defaultCountersDocument = new CountersDocument();
				var t1 = store.CreateCounterAsync(defaultCountersDocument, expectedClientNames[0]);
				var t2 = store.CreateCounterAsync(defaultCountersDocument, expectedClientNames[1]);
				var t3 = store.CreateCounterAsync(defaultCountersDocument, expectedClientNames[2]);

				await Task.WhenAll(t1, t2, t3);

				var counterStorageNames = await store.GetCounterStoragesNamesAsync();
				counterStorageNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}


		[Fact]
		public async Task Should_not_be_able_to_create_counter_with_the_same_name_twice()
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterAsync(new CountersDocument(),CounterName);

				//invoking create counter with the same name twice should fail
				store.Invoking(c => c.CreateCounterAsync(new CountersDocument(),CounterName).Wait())
					 .ShouldThrow<InvalidOperationException>();
			}
		}
	}
}
