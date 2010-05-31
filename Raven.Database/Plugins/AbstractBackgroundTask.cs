using System;
using System.ComponentModel.Composition;
using System.Threading;
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

			new Thread(BackgroundTask)
			{
				Name = "Background task " + GetType().Name,
				IsBackground = true
			}.Start();
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
					context.WaitForWork(TimeSpan.FromMinutes(1));
			}
		}

		protected abstract bool HandleWork();
	}
}