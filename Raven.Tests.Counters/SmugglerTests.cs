using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Xunit;

namespace Raven.Tests.Counters
{
	public class SmugglerTests : RavenBaseCountersTest
	{
		private const string CounterDumpFilename = "testCounter.counterdump";

		[Fact]
		public async Task SmugglerExport_should_work()
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
	}
}
