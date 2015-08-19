using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Counters;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Xunit;
using Xunit.Extensions;
using Xunit.Sdk;

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
				deltas.Should().ContainSingle(x => x.CounterName == "c1" && x.GroupName == "g1");
				deltas.Should().ContainSingle(x => x.CounterName == "c2" && x.GroupName == "g1");
				deltas.Should().ContainSingle(x => x.CounterName == "c" && x.GroupName == "g");

				deltas.First(x => x.CounterName == "c1" && x.GroupName == "g1").Value.Should().Be(5);
				deltas.First(x => x.CounterName == "c2" && x.GroupName == "g1").Value.Should().Be(1);

				await counterStore.ChangeAsync("g1", "c2",1);
				await counterStore.ChangeAsync("g", "c",-2);

				deltas = await counterStore.Advanced.GetCounterStatesSinceEtag(deltas.Max(x => x.Etag));

				deltas.First(x => x.CounterName == "c" && x.GroupName == "g").Value.Should().Be(-2);
				deltas.First(x => x.CounterName == "c2" && x.GroupName == "g1").Value.Should().Be(2);

				await counterStore.ChangeAsync("g", "c", -3);
				deltas = await counterStore.Advanced.GetCounterStatesSinceEtag(deltas.Max(x => x.Etag));
				deltas.First(x => x.CounterName == "c" && x.GroupName == "g").Value.Should().Be(-3);
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
					summary.Should().HaveCount(2);
					summary.Should().ContainSingle(x => x.Total == 1 && x.GroupName == "G" && x.CounterName == "C");
					summary.Should().ContainSingle(x => x.Total == -1 && x.GroupName == "G" && x.CounterName == "C2");
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
