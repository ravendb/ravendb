using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Util;
using Raven.Client.Counters;
using Raven.Database.Counters;
using Raven.Database.Extensions;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Counters
{
	public class CountersBasicClientTests : RavenBaseCountersTest
	{
		private const string CounterStorageName = "FooBarCounterStore";
		private const string CounterName = "FooBarCounter";
		private const string CounterDumpFilename = "Counter.Dump";

		[Fact]
		public async Task SmugglerImport_incremental_from_file_should_work()
		{
			IOExtensions.DeleteDirectory(CounterDumpFilename); //counters incremental export creates folder with incremental dump files

			using (var counterStore = NewRemoteCountersStore("storeToExport"))
			{
				await counterStore.ChangeAsync("g1", "c1", 5);
				await counterStore.IncrementAsync("g1", "c2");
				await counterStore.IncrementAsync("g", "c");

				var deltas = await counterStore.Advanced.GetCounterStatesSinceEtag(0);
				Assert.NotNull(deltas.SingleOrDefault(x => x.CounterName == "c1" && x.GroupName == "g1"));
                Assert.NotNull(deltas.SingleOrDefault(x => x.CounterName == "c2" && x.GroupName == "g1"));
                Assert.NotNull(deltas.SingleOrDefault(x => x.CounterName == "c" && x.GroupName == "g"));

                Assert.Equal(5, deltas.First(x => x.CounterName == "c1" && x.GroupName == "g1").Value);
			    Assert.Equal(1, deltas.First(x => x.CounterName == "c2" && x.GroupName == "g1").Value);

				await counterStore.ChangeAsync("g1", "c2",1);
				await counterStore.ChangeAsync("g", "c",-2);

				deltas = await counterStore.Advanced.GetCounterStatesSinceEtag(deltas.Max(x => x.Etag));

				var deltaForGandC = deltas.First(x => x.CounterName == "c" && x.GroupName == "g");
				Assert.True(deltaForGandC.Value == 2 && deltaForGandC.Sign == ValueSign.Negative);
				var deltaForG1andC2 = deltas.First(x => x.CounterName == "c2" && x.GroupName == "g1");
				Assert.True(deltaForG1andC2.Value == 2 && deltaForG1andC2.Sign == ValueSign.Positive);

				await counterStore.ChangeAsync("g", "c", -3);
				deltas = await counterStore.Advanced.GetCounterStatesSinceEtag(deltas.Max(x => x.Etag));
				deltaForGandC = deltas.First(x => x.CounterName == "c" && x.GroupName == "g");
				Assert.True(deltaForGandC.Value == 5 && deltaForGandC.Sign == ValueSign.Negative);
			}
		}

		[Fact]
		public async Task CountersInitialize_with_EnsureDefaultCounterCreation_should_not_overwrite_existing_counters()
		{
			using (var server = GetNewServer(port:8091))
			{
				using (var counterStore = new CounterStore
				{
					Url = string.Format("{0}:{1}", server.DocumentStore.Url, server.Configuration.Port),
					Name = DefaultCounterStorageName					
				})
				{
					counterStore.Initialize(true);

					await counterStore.IncrementAsync("G", "C");
					await counterStore.DecrementAsync("G", "C2");
				}

				using (var counterStore = new CounterStore
				{
					Url = string.Format("{0}:{1}", server.DocumentStore.Url, server.Configuration.Port),
					Name = DefaultCounterStorageName
				})
				{
					counterStore.Initialize(true);
					var summary = await counterStore.Admin.GetCounterStorageSummary(DefaultCounterStorageName);
                    Assert.Equal(2, summary.Length);
                    Assert.NotNull(summary.SingleOrDefault(x => x.Total == 1 && x.GroupName == "G" && x.CounterName == "C"));
					Assert.NotNull(summary.SingleOrDefault(x => x.Total == -1 && x.GroupName == "G" && x.CounterName == "C2"));
				}
			}
		}

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
                Assert.Equal(total, delta);
				
				await store.ResetAsync(counterGroupName, CounterName);

				total = await store.GetOverallTotalAsync(counterGroupName, CounterName);
				Assert.Equal(0, total);
			}	
		}

		[Theory]
		[InlineData(2)]
		[InlineData(-2)]
		[InlineData(5)]
		[InlineData(-7)]
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
			    Assert.Equal(total, delta);

			    AsyncHelpers.RunSync(() => store.DeleteAsync(counterGroupName, CounterName));
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
				Assert.Equal(1, total);

				await store.IncrementAsync(CounterGroupName, CounterName);

				total = await store.GetOverallTotalAsync(CounterGroupName, CounterName);
				Assert.Equal(2, total);
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
				Assert.Equal(5, total);

				await store.ChangeAsync(CounterGroupName, CounterName, -30);

				total = await store.GetOverallTotalAsync(CounterGroupName, CounterName);
				Assert.Equal(-25,total);
			}
		}
	}
}
