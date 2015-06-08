using System;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Xunit;

namespace Raven.Tests.Counters
{
	public class AdminClientTests : RavenBaseCountersTest
	{
		private const string CounterStorageName = "FooBarCounter";

		[Fact]
		public async Task Should_be_able_to_create_counter_storage()
		{
			using (var store = NewRemoteCountersStore(DefaultCounteStorageName,createDefaultCounter:false))
			{
				await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument(), CounterStorageName);

				var counterStorageNames = await store.Admin.GetCounterStoragesNamesAsync();
				counterStorageNames.Should().HaveCount(1)
									.And.Contain(CounterStorageName);
			}
		}

		[Fact]
		public async Task Should_be_able_to_create_multiple_counter_storages()
		{
			var expectedClientNames = new[] { CounterStorageName + "A", CounterStorageName + "B", CounterStorageName + "C" };
			using (var store = NewRemoteCountersStore(DefaultCounteStorageName,createDefaultCounter: false))
			{
				var defaultCountersDocument = new CounterStorageDocument();
				await store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[0]);
				await store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[1]);
				await store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[2]);

				var counterStorageNames = await store.Admin.GetCounterStoragesNamesAsync();
				counterStorageNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}

		[Fact]
		public async Task Should_be_able_to_delete_counter_storages()
		{
			var expectedClientNames = new[] { CounterStorageName + "A", CounterStorageName + "C" };
			using (var store = NewRemoteCountersStore(DefaultCounteStorageName, createDefaultCounter: false))
			{
				await store.Admin.CreateCounterStorageAsync(CreateCounterStorageDocument(expectedClientNames[0]), expectedClientNames[0]);
				await store.Admin.CreateCounterStorageAsync(CreateCounterStorageDocument("CounterThatWillBeDeleted"), "CounterThatWillBeDeleted");
				await store.Admin.CreateCounterStorageAsync(CreateCounterStorageDocument(expectedClientNames[1]), expectedClientNames[1]);

				await store.Admin.DeleteCounterStorageAsync("CounterThatWillBeDeleted", true);

				var counterStorageNames = await store.Admin.GetCounterStoragesNamesAsync();
				counterStorageNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}

		[Fact]
		public async Task Should_be_able_to_create_multiple_counter_storages_in_parallel()
		{
			var expectedClientNames = new[] { CounterStorageName + "A", CounterStorageName + "B", CounterStorageName + "C" };
			using (var store = NewRemoteCountersStore(DefaultCounteStorageName, createDefaultCounter: false))
			{
				var defaultCountersDocument = new CounterStorageDocument();
				var t1 = store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[0]);
				var t2 = store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[1]);
				var t3 = store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[2]);

				await Task.WhenAll(t1, t2, t3);

				var counterStorageNames = await store.Admin.GetCounterStoragesNamesAsync();
				counterStorageNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}


		[Fact]
		public async Task Should_not_be_able_to_create_counter_with_the_same_name_twice()
		{
			using (var store = NewRemoteCountersStore(DefaultCounteStorageName))
			{
				await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument(), CounterStorageName);

				//invoking create counter with the same name twice should fail
				store.Invoking(c => c.Admin.CreateCounterStorageAsync(new CounterStorageDocument(), CounterStorageName).Wait())
					 .ShouldThrow<InvalidOperationException>();
			}
		}
	}
}
