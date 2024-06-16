/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.ServiceProcess;
using System.Threading;

using log4net.Core;
[assembly: log4net.Config.XmlConfigurator(Watch=true)]

namespace Lucene.Net.Distributed.Operations
{
    /// <summary>
    /// A Windows service that provides system ping checking against LuceneServer.
    /// </summary>
	public class LuceneMonitor : System.ServiceProcess.ServiceBase
	{
		/// <summary> 
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.Container components = null;
		private ServiceController scMonitor = new ServiceController();
		private Thread serviceThread;
		private int sleepTime = 5000;
		private bool bRun = true;
		private string ipAddress = "";
		private int port = 0;
		private static readonly log4net.ILog oLog = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);


        public LuceneMonitor()
		{
			// This call is required by the Windows.Forms Component Designer.
			InitializeComponent();

			// TODO: Add any initialization after the InitComponent call
		}

		// The main entry point for the process
		static void Main()
		{
			System.ServiceProcess.ServiceBase[] ServicesToRun;
            ServicesToRun = new System.ServiceProcess.ServiceBase[] { new LuceneMonitor() };
			System.ServiceProcess.ServiceBase.Run(ServicesToRun);
		}

		/// <summary> 
		/// Required method for Designer support - do not modify 
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			components = new System.ComponentModel.Container();
			this.ServiceName = "LuceneMonitor";
		}

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		protected override void Dispose( bool disposing )
		{
			if( disposing )
			{
				if (components != null) 
				{
					components.Dispose();
				}
			}
			base.Dispose( disposing );
		}

		/// <summary>
		/// Set things in motion so your service can do its work.
		/// </summary>
		protected override void OnStart(string[] args)
		{
			ThreadStart threadStart = new ThreadStart(MonitorService);
			serviceThread = new Thread(threadStart);
			serviceThread.Start();
		}

		private void LogMessage(string message)
		{
			this.LogMessage(message, Level.Info);
		}
		private void LogMessage(string message, Level msgLevel)
		{
			if (msgLevel==Level.Info)
			{
				if (oLog.IsInfoEnabled)
					oLog.Info(message);
			}
			else if (msgLevel==Level.Warn)
			{
				if (oLog.IsWarnEnabled)
					oLog.Warn(message);
			}
		}
		private void LogMessage(string message, Level msgLevel, int ErrorLevel)
		{
			if (msgLevel==Level.Error)
			{
				if (oLog.IsErrorEnabled)
				{
					oLog.Error(message);
					EventLog.WriteEntry(this.ServiceName, message, EventLogEntryType.Error, ErrorLevel);
				}
			}
		}

		private void MonitorService()
		{
			this.LogMessage(this.ServiceName+" started");
			scMonitor.ServiceName="LuceneServer";
            this.sleepTime = (ConfigurationManager.AppSettings["ServiceSleepTime"] != null ? Convert.ToInt32(ConfigurationManager.AppSettings["ServiceSleepTime"]) : this.sleepTime);
            this.ipAddress = (ConfigurationManager.AppSettings["IPAddress"] != null ? ConfigurationManager.AppSettings["IPAddress"] : "");
            this.port = (ConfigurationManager.AppSettings["Port"] != null ? Convert.ToInt32(ConfigurationManager.AppSettings["Port"]) : 0);
			this.LogMessage("ServiceSleepTime = "+this.sleepTime.ToString()+"; ipAddress="+this.ipAddress+"; port="+this.port.ToString());

			while (bRun)
			{
				this.CheckService();
				Thread.Sleep(sleepTime);
			}
		}

		private void CheckService()
		{
			try
			{
				scMonitor.Refresh();

				if (scMonitor.Status.Equals(ServiceControllerStatus.StopPending))
					scMonitor.WaitForStatus(ServiceControllerStatus.Stopped);

				if (scMonitor.Status.Equals(ServiceControllerStatus.Stopped))
				{
					// Start the service if the current status is stopped.
					foreach (IChannel ic in ChannelServices.RegisteredChannels)
						ChannelServices.UnregisterChannel(ic);
					scMonitor.Start();
					this.LogMessage(scMonitor.ServiceName + " started (Service stopped or StopPending)", Level.Error, 99);
				}
			}
			catch (Exception e)
			{
				this.LogMessage(scMonitor.ServiceName + " error: "+e.Message+e.StackTrace, Level.Error, 199);
			}

		}


		/// <summary>
		/// Stop this service.
		/// </summary>
		protected override void OnStop()
		{
			this.bRun=false;
		}
	}
}
