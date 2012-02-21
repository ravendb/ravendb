//-----------------------------------------------------------------------
// <copyright file="TasksExecuter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using NLog;
using Raven.Database.Storage;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
	public class TasksExecuter
	{
		private readonly WorkContext context;
		private static readonly Logger log = LogManager.GetCurrentClassLogger();
		private readonly ITransactionalStorage transactionalStorage;
		private int workCounter;

		public TasksExecuter(ITransactionalStorage transactionalStorage, WorkContext context)
		{
			this.transactionalStorage = transactionalStorage;
			this.context = context;
		}

		public void Execute()
		{
			var name = GetType().Name;
			while (context.DoWork)
			{
				var foundWork = false;
				try
				{
					foundWork = ExecuteTasks();
				}
				catch (Exception e)
				{
					log.ErrorException("Failed to execute indexing", e);
				}
				if (foundWork == false)
				{
					context.WaitForWork(TimeSpan.FromHours(1), ref workCounter, name);
				}
			}
		}

		private bool ExecuteTasks()
		{
			Task task = null;
			transactionalStorage.Batch(actions =>
			{
				int tasks;
				task = actions.Tasks.GetMergedTask(out tasks);
			});

			if (task == null)
				return false;


			log.Debug("Executing {0}", task);

			try
			{
				task.Execute(context);
			}
			catch (Exception e)
			{
				log.WarnException(
					string.Format("Task {0} has failed and was deleted without completing any work", task),
					e);
			}
			return true;
		}

	}
}