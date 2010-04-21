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
				string taskAsJson = null;
				try
				{
					transactionalStorage.Batch(actions =>
					{
						log.Debug("Trying to find a task to execute");
						var taskAndBookMark = actions.GetFirstTask();
						if (taskAndBookMark == null)
						{
							log.Debug("Could not find any task to execute, will wait for more work");
							actions.Commit();
							return;
						}
						taskAsJson = taskAndBookMark.Item1;
						log.DebugFormat("Executing {0}", taskAsJson);
						foundWork = true;

						ExecuteTask(taskAsJson);

						actions.CompleteCurrentTask(taskAndBookMark.Item2);
						actions.Commit();
					});
				}
				catch (Exception e)
				{
#if DEBUG
					Debugger.Launch();
#endif
					log.Error("Failed to execute task: " + taskAsJson, e);
				}
				if (foundWork == false)
					context.WaitForWork();
			}
		}

		private void ExecuteTask(string taskAsJson)
		{
			try
			{
				var task = Task.ToTask(taskAsJson);
				try
				{
					task.Execute(context);
				}
				catch (Exception e)
				{
					log.WarnFormat(e, "Task {0} has failed and was deleted without completing any work", taskAsJson);
				}
			}
			catch (Exception e)
			{
				log.ErrorFormat(e, "Could not create instance of a task: {0}", taskAsJson);
			}
		}
	}
}