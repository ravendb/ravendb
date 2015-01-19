//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading;
//using System.Threading.Tasks;
//using FluentAssertions;
//using Raven.Abstractions.Counters;
//using Raven.Abstractions.Data;
//using Raven.Client.Counters;
//using Xunit;
//using Xunit.Extensions;
//
//namespace Raven.Tests.Counters
//{
//	public class CountersBatchTests : BaseCountersTest
//	{
//		private const string CounterStorageName = "FooBarCounter";
//
//		[Fact]
//		public async Task CountersBatch_should_work()
//		{
//			using (var server = GetNewServer(port:9000))
//			using (var store = NewRemoteDocumentStore(ravenDbServer:server, fiddler: true))
//			using (var countersClient = store.NewCountersClient(CounterStorageName))
//			{
//				await countersClient.Admin.CreateCounterAsync(new CountersDocument()
//				{
//					Settings = new Dictionary<string, string>
//					{
//						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
//					},
//				},CounterStorageName);
//				using (var counterBatch = countersClient.NewBatch(new CountersBatchOptions{ BatchSizeLimit = 3 }))
//				{
//					counterBatch.Increment("FooGroup", CounterStorageName);
//					counterBatch.Increment("FooGroup", CounterStorageName);
//					counterBatch.Decrement("FooGroup", CounterStorageName);
//				}
//
//				var total = await countersClient.Commands.GetOverallTotalAsync("FooGroup", CounterStorageName);
//				total.Should().Be(1);
//			}
//		}
//
//		[Theory]
//		[InlineData(10)]
//		[InlineData(50)]
//		public async Task CountersBatch_with_multiple_batches_should_work(int countOfOperationsInBatch)
//		{
//			using (var server = GetNewServer(port: 9000))
//			using (var store = NewRemoteDocumentStore(ravenDbServer: server, fiddler: true))
//			using (var countersClient = store.NewCountersClient(CounterStorageName))
//			{
//				await countersClient.Admin.CreateCounterAsync(new CountersDocument()
//				{
//					Settings = new Dictionary<string, string>
//					{
//						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
//					},
//				}, CounterStorageName);
//				using (var counterBatch = countersClient.NewBatch(new CountersBatchOptions { BatchSizeLimit = countOfOperationsInBatch / 4 }))
//				{
//					for(int i = 0; i < countOfOperationsInBatch; i++)
//						counterBatch.Increment("FooGroup", CounterStorageName);
//				}
//
//				var total = await countersClient.Commands.GetOverallTotalAsync("FooGroup", CounterStorageName);
//				total.Should().Be(countOfOperationsInBatch);
//			}
//			
//		}
//	}
//}
