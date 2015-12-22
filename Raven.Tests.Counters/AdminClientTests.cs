using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Counters
{
    public class AdminClientTests : RavenBaseCountersTest
    {
        private const string CounterStorageName = "FooBarCounter";

        [Fact]
        public async Task Should_be_able_to_create_counter_storage()
        {
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName,createDefaultCounter:false))
            {
                await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument(), CounterStorageName);

                var counterStorageNames = await store.Admin.GetCounterStoragesNamesAsync();
                Assert.Equal(1, counterStorageNames.Count);
                Assert.Contains(CounterStorageName, counterStorageNames);
            }
        }

		[Fact]
		public async Task CounterStorageExists_should_work1()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName, createDefaultCounter: false))
			{
				await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument(), CounterStorageName);
				Assert.True(await store.Admin.CounterStorageExists(CounterStorageName));
			}
		}

		[Fact]
		public async Task CounterStorageExists_should_work2()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
				Assert.True(await store.Admin.CounterStorageExists());
		}

		[Fact]
		public async Task CounterStorageExists_should_work3()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName, createDefaultCounter: false))
				Assert.False(await store.Admin.CounterStorageExists(CounterStorageName));
		}

		[Fact]
        public async Task Should_be_able_to_create_multiple_counter_storages()
        {
            var expectedClientNames = new ReadOnlyCollection<string>(new List<string>()
            {
                CounterStorageName + "A", CounterStorageName + "B", CounterStorageName + "C"
            });
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName,createDefaultCounter: false))
            {
                var defaultCountersDocument = new CounterStorageDocument();
                await store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[0]);
                await store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[1]);
                await store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[2]);

                var counterStorageNames = (await store.Admin.GetCounterStoragesNamesAsync());
                Assert.Equal(counterStorageNames, expectedClientNames);
            }
        }

        [Fact]
        public async Task Should_be_able_to_delete_counter_storages()
        {
            var expectedClientNames = new ReadOnlyCollection<string>(new List<string>()
            {
                CounterStorageName + "A", CounterStorageName + "C"
            });
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName, createDefaultCounter: false))
            {
                await store.Admin.CreateCounterStorageAsync(MultiDatabase.CreateCounterStorageDocument(expectedClientNames[0]), expectedClientNames[0]);
                await store.Admin.CreateCounterStorageAsync(MultiDatabase.CreateCounterStorageDocument("CounterThatWillBeDeleted"), "CounterThatWillBeDeleted");
                await store.Admin.CreateCounterStorageAsync(MultiDatabase.CreateCounterStorageDocument(expectedClientNames[1]), expectedClientNames[1]);

                await store.Admin.DeleteCounterStorageAsync("CounterThatWillBeDeleted", true);

                var counterStorageNames = await store.Admin.GetCounterStoragesNamesAsync();
                Assert.Equal(counterStorageNames, expectedClientNames);
            }
        }

        [Fact]
        public async Task Should_be_able_to_create_multiple_counter_storages_in_parallel()
        {
            var expectedClientNames = new ReadOnlyCollection<string>(new List<string>()
            {
                CounterStorageName + "A", CounterStorageName + "B", CounterStorageName + "C"
            });
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName, createDefaultCounter: false))
            {
                var defaultCountersDocument = new CounterStorageDocument();
                var t1 = store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[0]);
                var t2 = store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[1]);
                var t3 = store.Admin.CreateCounterStorageAsync(defaultCountersDocument, expectedClientNames[2]);

                await Task.WhenAll(t1, t2, t3);

                var counterStorageNames = await store.Admin.GetCounterStoragesNamesAsync();
                Assert.Equal(counterStorageNames, expectedClientNames);
            }
        }


        [Fact]
        public async Task Should_not_be_able_to_create_counter_with_the_same_name_twice()
        {
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
            {
                await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument(), CounterStorageName);

                //invoking create counter with the same name twice should fail
                Assert.Throws<InvalidOperationException>(() => 
                    store.Admin.CreateCounterStorageAsync(new CounterStorageDocument(), CounterStorageName).GetAwaiter().GetResult());
            }
        }
    }
}
