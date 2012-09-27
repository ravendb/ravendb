//-----------------------------------------------------------------------
// <copyright file="AbstractBackgroundTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;

namespace Raven.Database.Plugins
{
	public abstract class AbstractBackgroundTask : IStartupTask
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		public DocumentDatabase Database { get; set; }

		public void Execute(DocumentDatabase database)
		{
			Database = database;
		    Initialize();
		    Task.Factory.StartNew(BackgroundTask,TaskCreationOptions.LongRunning);
		}

	    protected virtual void Initialize()
	    {
	    }

		int workCounter;
		public void BackgroundTask()
		{
			var name = GetType().Name;
			var context = Database.WorkContext;
			while (context.DoWork)
			{
				var foundWork = false;
				try
				{
					foundWork = HandleWork();
				}
				catch (Exception e)
				{
					log.ErrorException("Failed to execute background task", e);
				}
				if (foundWork == false)
				{
					context.WaitForWork(TimeoutForNextWork(), ref workCounter, name);
				}
				else
				{
					context.UpdateFoundWork();
				}
			}
		}

	    protected virtual TimeSpan TimeoutForNextWork()
	    {
	        return TimeSpan.FromHours(1);
	    }

	    protected abstract bool HandleWork();
	}
}
