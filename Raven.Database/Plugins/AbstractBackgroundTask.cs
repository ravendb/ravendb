using System;
using System.ComponentModel.Composition;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public abstract class AbstractBackgroundTask : IStartupTask
	{
		private readonly ILog log;

		protected AbstractBackgroundTask()
		{
			log = LogManager.GetLogger(GetType());
		}

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

	    public void BackgroundTask()
		{
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
					log.Error("Failed to execute background task", e);
				}
				if (foundWork == false)
					context.WaitForWork(TimeSpan.FromHours(1));
			}
		}

		protected abstract bool HandleWork();
	}
}
