using System;
using System.Diagnostics;
using log4net;
using Raven.Database.Extensions;
using Raven.Database.Storage;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
	public class TaskExecuter
	{
		private readonly WorkContext context;
		private readonly ILog log = LogManager.GetLogger(typeof (TaskExecuter));
		private readonly TransactionalStorage transactionalStorage;

		public TaskExecuter(TransactionalStorage transactionalStorage, WorkContext context)
		{
			this.transactionalStorage = transactionalStorage;
			this.context = context;
		}

		public void Execute()
		{
			while (context.DoWork)
			{
				var foundWork = false;
				Task task = null;
				try
				{
					int tasks = 0;
					transactionalStorage.Batch(actions =>
					{
						task = actions.Tasks.GetMergedTask(out tasks);
						if (task == null)
							return;

						log.DebugFormat("Executing {0}", task);
						foundWork = true;

						try
						{
							task.Execute(context);
						}
						catch (Exception e)
						{
							log.WarnFormat(e, "Task {0} has failed and was deleted without completing any work", task);
						}
					});
					context.PerformanceCounters.IncrementProcessedTask(tasks);
				}
				catch (Exception e)
				{
					log.Error("Failed to execute task: " + task, e);
				}
				if (foundWork == false)
					context.WaitForWork(TimeSpan.FromSeconds(1));
			}
		}
	}
}