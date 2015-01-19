//using System;
//using System.Threading.Tasks;
//using FluentAssertions;
//using Raven.Abstractions.Counters;
//using Raven.Client.Counters;
//using Raven.Client.Extensions;
//using Raven.Tests.Helpers;
//using Xunit;
//
//namespace Raven.Tests.Counters
//{
//	public class AdminClientTests : RavenTestBase
//	{
//		private const string CounterName = "FooBarCounter";
//
//		[Fact]
//		public void Should_be_able_to_initialize()
//		{
//			Assert.DoesNotThrow(() =>
//			{
//				using (var counterStore = new CounterStore
//				{
//					Url,Credentials, ApiKey, CounterName, 
//				})
//				{
//					counterStore.EnsureCounterExists(new CountersDocument());
//					using (var client = counterStore.NewClient())
//						client.Should().NotBeNull();
//				}
//			});
//		}
//
//		[Fact]
//		public async Task Should_be_able_to_create_storage()
//		{
//			using (var store = NewRemoteDocumentStore())
//			{
//				
//				using (var client = store.NewCountersClient(CounterName))
//				{
//					await client.Admin.CreateCounterAsync(new CountersDocument(),"AA");
//					var counterStorageNames = await client.Admin.GetCounterStoragesNamesAsync();
//					counterStorageNames.Should().HaveCount(1)
//						.And.Contain(CounterName);
//				}
//			}
//		}
//		
//		[Fact]
//		public async Task Should_be_able_to_create_multiple_storages()
//		{
//			var expectedClientNames = new[] { CounterName + "A", CounterName + "B", CounterName + "C" };
//			using (var store = NewRemoteDocumentStore())
//			using (var client = store.NewCountersClient(CounterName))
//			{
//				var defaultCountersDocument = new CountersDocument();
//				await client.Admin.CreateCounterAsync(defaultCountersDocument , expectedClientNames[0]);
//				await client.Admin.CreateCounterAsync(defaultCountersDocument , expectedClientNames[1]);
//				await client.Admin.CreateCounterAsync(defaultCountersDocument , expectedClientNames[2]);
//				
//				var counterStorageNames = await client.Admin.GetCounterStoragesNamesAsync();				
//				counterStorageNames.Should().BeEquivalentTo(expectedClientNames);				
//			}
//		}
//
//		[Fact]
//		public async Task Should_not_be_able_to_create_storage_with_the_same_name_twice()
//		{
//			using (var store = NewRemoteDocumentStore())
//			using (var client = store.NewCountersClient(CounterName))
//			{
//				await client.Admin.CreateCounterAsync(new CountersDocument());
//				client.Admin.Invoking(c =>  c.CreateCounterAsync(new CountersDocument()).Wait())
//							.ShouldThrow<InvalidOperationException>();
//			}
//		}
//	}
//}
