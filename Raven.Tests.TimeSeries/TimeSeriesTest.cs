using Raven.Abstractions.TimeSeries;
using Raven.Database.Config;
using Raven.Database.TimeSeries;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesTest
	{
		public static TimeSeriesStorage GetStorage()
		{
			var storage = new TimeSeriesStorage("http://localhost:8080/", "TimeSeriesTest", new RavenConfiguration { RunInMemory = true });
			using (var writer = storage.CreateWriter())
			{
				writer.CreateType("Simple", new[] {"Value"});
                writer.Commit();
            }
			return storage;
		}
	}
}