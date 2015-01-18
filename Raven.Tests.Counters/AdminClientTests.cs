using System;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Raven.Client.Counters;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Counters
{
	public class AdminClientTests : RavenTestBase
	{
		private const string CounterStorageName = "FooBarCounter";

		[Fact]
		public void Should_be_able_to_initialize()
		{
			Assert.DoesNotThrow(() =>
			{
				using (var store = NewRemoteDocumentStore())
				using (var client = store.NewCountersClient(CounterStorageName))
					client.Should().NotBeNull();
			});
		}

		[Fact]
		public async Task Should_be_able_to_create_storage()
		{
				using (var store = NewRemoteDocumentStore())
				using (var client = store.NewCountersClient(CounterStorageName))
				{
					await client.Admin.CreateCounterStorageAsync(new CountersDocument());
					var counterStorageNames = await client.Admin.GetCounterStoragesNamesAsync();
					counterStorageNames.Should().HaveCount(1)
											.And.Contain(CounterStorageName);
				}			
		}
		
		[Fact]
		public async Task Should_be_able_to_create_multiple_storages()
		{
			var expectedClientNames = new[] { CounterStorageName + "A", CounterStorageName + "B", CounterStorageName + "C" };
			using (var store = NewRemoteDocumentStore())
			using (var client = store.NewCountersClient(CounterStorageName))
			{
				var defaultCountersDocument = new CountersDocument();
				await client.Admin.CreateCounterStorageAsync(defaultCountersDocument , expectedClientNames[0]);
				await client.Admin.CreateCounterStorageAsync(defaultCountersDocument , expectedClientNames[1]);
				await client.Admin.CreateCounterStorageAsync(defaultCountersDocument , expectedClientNames[2]);
				
				var counterStorageNames = await client.Admin.GetCounterStoragesNamesAsync();				
				counterStorageNames.Should().BeEquivalentTo(expectedClientNames);				
			}
		}

		[Fact]
		public async Task Should_not_be_able_to_create_storage_with_the_same_name_twice()
		{
			using (var store = NewRemoteDocumentStore())
			using (var client = store.NewCountersClient(CounterStorageName))
			{
				await client.Admin.CreateCounterStorageAsync(new CountersDocument());
				client.Admin.Invoking(c =>  c.CreateCounterStorageAsync(new CountersDocument()).Wait())
							.ShouldThrow<InvalidOperationException>();
			}
		}
	}
}
