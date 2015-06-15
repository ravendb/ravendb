using Raven.Database.Config;
using Raven.Database.TimeSeries;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesTest
	{
		public static TimeSeriesStorage GetStorage()
		{
			return new TimeSeriesStorage("http://localhost:8080", "TimeSeriesTest", new RavenConfiguration { RunInMemory = true });
		}
	}
}