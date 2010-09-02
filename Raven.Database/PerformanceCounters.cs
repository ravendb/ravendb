using System;
using System.Diagnostics;
using log4net;

namespace Raven.Database
{
	/// <summary>
	/// Because perf counters may require permissions, they are consider optional
	/// and errors using them will not result in any problems when running the server
	/// </summary>
	public class PerformanceCounters
	{
		private readonly bool recordPerfCounter = true;
		private readonly PerformanceCounter numberOfTasksPerSecond;

		private static readonly ILog logger = LogManager.GetLogger(typeof (PerformanceCounters));

		public void IncrementProcessedTask(int numberOfTasks)
		{
			if (recordPerfCounter == false)
				return;
			numberOfTasksPerSecond.IncrementBy(numberOfTasks);
		}

		public PerformanceCounters(string instnaceName)
		{
			try
			{
				numberOfTasksPerSecond = new PerformanceCounter("RavenDB", "# of tasks / sec", instnaceName, false);
			}
			catch (Exception e)
			{
				logger.Warn("Could not initailize performance counter: " + instnaceName +". Performance counters will be disabled", e);
				recordPerfCounter = false;
			}
		}
	}
}