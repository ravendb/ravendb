//-----------------------------------------------------------------------
// <copyright file="RavenService.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Reflection;
using System.ServiceProcess;
using Raven.Database.Config;
using Task = System.Threading.Tasks.Task;

namespace Raven.Server
{
	internal partial class RavenService : ServiceBase
	{
		const int SERVICE_ACCEPT_PRESHUTDOWN = 0x100;
		const int SERVICE_CONTROL_PRESHUTDOWN = 0xf;

		private RavenDbServer server;
		private Task startTask;

		public RavenService()
		{
			InitializeComponent();
			AcceptPreshutdown();
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
			var shutdownTask = startTask.ContinueWith(task =>
			{
				if(server != null) 
					server.Dispose();
				return task;
			});
			var keepAliveTask = Task.Factory.StartNew(() => 
			{
				if(shutdownTask.Wait(9000))
					return;
				do 
				{
					RequestAdditionalTime(10000);
				} while(!shutdownTask.Wait(9000));
			});

			Task.WaitAll(shutdownTask, keepAliveTask);

		}

		protected override void OnShutdown()
		{
			var shutdownTask = startTask.ContinueWith(task =>
			{
				if (server != null)
					server.Dispose();
				return task;
			});
			Task.WaitAll(shutdownTask);
		}

		protected override void OnCustomCommand(int command)
		{
			base.OnCustomCommand(command);

			if (command != SERVICE_CONTROL_PRESHUTDOWN) return;

			this.Stop();
		}

		private void AcceptPreshutdown()
		{
			// http://www.sivachandran.in/2012/03/handling-pre-shutdown-notification-in-c.html
			FieldInfo acceptedCommandsFieldInfo = typeof(ServiceBase).GetField("acceptedCommands", BindingFlags.Instance | BindingFlags.NonPublic);
			if (acceptedCommandsFieldInfo == null)
				return;

			int value = (int)acceptedCommandsFieldInfo.GetValue(this);
			acceptedCommandsFieldInfo.SetValue(this, value | SERVICE_ACCEPT_PRESHUTDOWN);
		}
	}
}
