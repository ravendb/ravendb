using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Extensions;
using Raven.Database.Smuggler;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class SmugglerTests : RavenBaseTimeSeriesTest
	{
		private const string TimeSeriesDumpFilename = "testTimeSeries.timeseriesdump";

		[Fact]
		public async Task SmugglerExport_should_work()
		{
			IOExtensions.DeleteFile(TimeSeriesDumpFilename);

			using (var timeSeriesStore = NewRemoteTimeSeriesStore("store"))
			{
				await timeSeriesStore.ChangeAsync("g1", "c1", 5);
				await timeSeriesStore.IncrementAsync("g1", "c1");
				await timeSeriesStore.IncrementAsync("g1", "c2");
				await timeSeriesStore.IncrementAsync("g2", "c1");

				var smugglerApi = new SmugglerTimeSeriesApi(timeSeriesStore);
				
				await smugglerApi.ExportData(new SmugglerExportOptions<TimeSeriesConnectionStringOptions>
				{
					ToFile = TimeSeriesDumpFilename
				});

				var fileInfo = new FileInfo(TimeSeriesDumpFilename);
				fileInfo.Exists.Should().BeTrue();
				fileInfo.Length.Should().BeGreaterThan(0);
			}
		}
	}
}
