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
			startTask.ContinueWith(task =>
			{
				if (server != null)
					server.Dispose();
				return task;
			}).Wait();

		}
	}
}
