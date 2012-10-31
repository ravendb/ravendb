//-----------------------------------------------------------------------
// <copyright file="RavenService.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.ServiceProcess;
using Raven.Database.Config;
using Task = System.Threading.Tasks.Task;

namespace Raven.Server
{
	internal partial class RavenService : ServiceBase
	{
		private RavenDbServer server;
		private Task startTask;

		public RavenService()
		{
			InitializeComponent();
		}

		protected override void OnStart(string[] args)
		{
			startTask = Task.Factory.StartNew(() =>
			{
				try
				{
					server = new RavenDbServer(new RavenConfiguration());
				}
				catch (Exception e)
				{
					EventLog.WriteEntry("RavenDB service failed to start because of an error" + Environment.NewLine + e, EventLogEntryType.Error);
					Stop();
				}
			});

			if(startTask.Wait(TimeSpan.FromSeconds(20)) == false)
			{
				EventLog.WriteEntry(
					"Startup for RavenDB service seems to be taking longer than usual, moving initialization to a background thread",
					EventLogEntryType.Warning);
			}

		}

		protected override void OnStop()
		{
			var complete = false;
			var shutdownStart = DateTime.Now;
			var shutdownTask = startTask.ContinueWith(task =>
			{
				if(server != null) 
					server.Dispose();
				complete = true;
				return task;
			});
			var keepAliveTask = Task.Factory.StartNew(() => 
			{
				System.Threading.Thread.Sleep(9000);
				do 
				{
					EventLog.WriteEntry("Requesting additional time for service stop: " + (int)((DateTime.Now - shutdownStart).TotalSeconds) + "s", EventLogEntryType.Information);
					base.RequestAdditionalTime(10000);
					System.Threading.Thread.Sleep(9000);
				} while(!complete);
			});

			Task.WaitAll(shutdownTask, keepAliveTask);

			shutdownTask.Dispose();
			keepAliveTask.Dispose();
		}
	}
}
