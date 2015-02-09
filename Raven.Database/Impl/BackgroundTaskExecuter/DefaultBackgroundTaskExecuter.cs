using Raven.Abstractions.Logging;
using Raven.Database.Config;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.Impl.BackgroundTaskExecuter;

namespace Raven.Database.Indexing
{
	public class DefaultBackgroundTaskExecuter : IBackgroundTaskExecuter, ICpuUsageHandler
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private double maxNumberOfParallelProcessingTasksRatio = 1;

		private RavenThreadPull _tp ;

		public DefaultBackgroundTaskExecuter()
		{
			CpuStatistics.RegisterCpuUsageHandler(this);
			var ct = new CancellationToken(false);
			// this is temporary!!1
			_tp = new RavenThreadPull(8, ct);
			_tp.Start();
		}

		/// <summary>
		/// Note that here we assume that  source may be very large (number of documents)
		/// </summary>
		public void ExecuteAllBuffered<T>(WorkContext context, IList<T> source, Action<IEnumerator<T>> action)
		{
			_tp.ExecuteBatch(source,action);
		}

		public void ExecuteAllInterleaved<T>(WorkContext context, IList<T> source, Action<T> action)
		{
			_tp.ExecuteBatch(source,action);
		}

		public void HandleHighCpuUsage()
		{
			maxNumberOfParallelProcessingTasksRatio = Math.Min(1, maxNumberOfParallelProcessingTasksRatio / 2);
		}

		public void HandleLowCpuUsage()
		{
			maxNumberOfParallelProcessingTasksRatio = Math.Min(1, maxNumberOfParallelProcessingTasksRatio * 1.2);
		}

		public double MaxNumberOfParallelProcessingTasksRatio
		{
			get { return maxNumberOfParallelProcessingTasksRatio; }
		}
	}
}