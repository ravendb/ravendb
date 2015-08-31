using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Raven.Abstractions.Exceptions;
using Raven.Monitor.IO;

namespace Raven.Monitor
{
	public class MonitorController: ApiController
	{
		[HttpGet]
		[Route("monitor/io-test")]
		public object MonitorIO()
		{
			var options = new MonitorOptions();
			var nvc = HttpUtility.ParseQueryString(Request.RequestUri.Query);
			var proccessId = nvc["process-id"];
			int pid;
			if (!string.IsNullOrEmpty(proccessId))
			{
				if(!int.TryParse(proccessId,out pid))
					throw new BadRequestException(string.Format("Could not parse pid: {0}", proccessId));
				options.ProcessId = pid;
			}
			else
			{
				var proc = Process.GetProcessesByName("Raven.Server");
				if (proc.Length != 1)
					throw new BadRequestException("More than one raven server is up and no pid was provided.");
				options.ProcessId = proc[0].Id;
			}
			options.Action = MonitorActions.DiskIo;
			var serverUrl = nvc["server-url"];

			if (string.IsNullOrEmpty(serverUrl))
			{
				serverUrl = "http://localhost:8080/";
			}
			try
			{
				WebRequest.Create(serverUrl + "build/version").GetResponse().Close();
			}
			catch (Exception)
			{
				throw new BadRequestException("Could not verify raven server url.");
			}
			options.ServerUrl = serverUrl;
			var durationStr = nvc["duration"];
			int durationInMinutes = 1;
			if (!string.IsNullOrEmpty(durationStr))
				int.TryParse(durationStr, out durationInMinutes);
			options.IoOptions.DurationInMinutes = durationInMinutes;
			Task.Factory.StartNew(() =>
			{
				using (var monitor = new DiskIoPerformanceMonitor(options))
					monitor.Start();
			});
			return options;
		}

		[HttpGet]
		[Route("monitor/start-monitoring")]
		public object MonitorAll()
		{
			MonitoringManager.MonitorManager.Start();
			return "Monitoring started";
		}

		[HttpGet]
		[Route("monitor/stop-monitoring")]
		public object StopMonitoring()
		{
			MonitoringManager.MonitorManager.Stop();
			return "Monitoring stoped";
		}
	}
}
