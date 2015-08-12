using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Extensions;
using Raven.Database.Indexing.Collation.Cultures;
using Raven.Database.Smuggler;
using Xunit;

namespace Raven.Tests.Counters
{
	public class SmugglerTests : RavenBaseCountersTest
	{
		private const string CounterDumpFilename = "testCounter.counterdump";

		[Fact]
		public async Task SmugglerExport_to_file_should_work()
		{
			IOExtensions.DeleteFile(CounterDumpFilename);

			using (var counterStore = NewRemoteCountersStore("store"))
			{
				await counterStore.ChangeAsync("g1", "c1", 5);
				await counterStore.IncrementAsync("g1", "c1");
				await counterStore.IncrementAsync("g1", "c2");
				await counterStore.IncrementAsync("g2", "c1");

				var smugglerApi = new SmugglerCounterApi(counterStore);
				
				await smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToFile = CounterDumpFilename
				});

				var fileInfo = new FileInfo(CounterDumpFilename);
				fileInfo.Exists.Should().BeTrue();
				fileInfo.Length.Should().BeGreaterThan(0);
			}
		}

		[Fact]
		public async Task SmugglerImport_from_file_should_work()
		{
			using (var counterStore = NewRemoteCountersStore("storeToExport"))
			{
				await counterStore.ChangeAsync("g1", "c1", 5);
				await counterStore.IncrementAsync("g1", "c1");
				await counterStore.IncrementAsync("g1", "c2");
				await counterStore.DecrementAsync("g2", "c1");

				var smugglerApi = new SmugglerCounterApi(counterStore);

				await smugglerApi.ExportData(new SmugglerExportOptions<CounterConnectionStringOptions>
				{
					ToFile = CounterDumpFilename
				});
			}

			using (var counterStore = NewRemoteCountersStore("storeToImportTo"))
			{
				var smugglerApi = new SmugglerCounterApi(counterStore);

				await smugglerApi.ImportData(new SmugglerImportOptions<CounterConnectionStringOptions>
				{
					FromFile = CounterDumpFilename,
					To = new CounterConnectionStringOptions
					{
						Url = counterStore.Url,
						CounterStoreId = counterStore.Name
					}
				});

				var summary = await counterStore.Admin.GetCounterStorageSummary(counterStore.Name);

				summary.Should().HaveCount(3); //sanity check
				summary.Should().ContainSingle(x => x.CounterName == "c1" && x.Group == "g1");
				summary.Should().ContainSingle(x => x.CounterName == "c2" && x.Group == "g1");
				summary.Should().ContainSingle(x => x.CounterName == "c1" && x.Group == "g2");

				summary.First(x => x.CounterName == "c1" && x.Group == "g1").Total.Should().Be(6); //change + inc
				summary.First(x => x.CounterName == "c2" && x.Group == "g1").Total.Should().Be(1);
				summary.First(x => x.CounterName == "c1" && x.Group == "g2").Total.Should().Be(-1);
			}

		}
	}
}
