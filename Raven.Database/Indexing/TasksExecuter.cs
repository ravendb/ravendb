//-----------------------------------------------------------------------
// <copyright file="TasksExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using log4net;
using Raven.Database.Extensions;
using Raven.Database.Storage;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
	public class TasksExecuter
	{
		private readonly WorkContext context;
		private readonly ILog log = LogManager.GetLogger(typeof (IndexingExecuter));
		private readonly ITransactionalStorage transactionalStorage;

		public TasksExecuter(ITransactionalStorage transactionalStorage, WorkContext context)
		{
			this.transactionalStorage = transactionalStorage;
			this.context = context;
		}

		int workCounter;
		
		public void Execute()
		{
			while (context.DoWork)
			{
				var foundWork = false;
				try
				{
					foundWork = ExecuteTasks();
				}
				catch (Exception e)
				{
					log.Error("Failed to execute indexing", e);
				}
				if (foundWork == false)
				{
					context.WaitForWork(TimeSpan.FromHours(1), ref workCounter);
				}
			}
		}


		private bool ExecuteTasks()
		{
			bool foundWork = false;
			int tasks = 0;
			transactionalStorage.Batch(actions =>
			                           	{
			                           		Task task = actions.Tasks.GetMergedTask(out tasks);
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
			return foundWork;
		}

	}
}