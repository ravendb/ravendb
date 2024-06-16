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
using System.Configuration.Install;
using System.ServiceProcess;
using Microsoft.Win32;

namespace LuceneMonitorInstall
{
	/// <summary>
	/// Summary description for ProjectInstaller.
	/// </summary>
	[RunInstallerAttribute(true)]
	public class ProjectInstaller : Installer
	{
		private ServiceProcessInstaller processInstaller;
		private ServiceInstaller serviceInstaller;
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.Container components = null;

		public ProjectInstaller()
		{
			// This call is required by the Designer.
			InitializeComponent();

			// TODO: Add any initialization after the InitializeComponent call
		}

		/// <summary> 
		/// Clean up any resources being used.
		/// </summary>
		protected override void Dispose( bool disposing )
		{
			if( disposing )
			{
				if(components != null)
				{
					components.Dispose();
				}
			}
			base.Dispose( disposing );
		}


		#region Component Designer generated code
		/// <summary>
		/// Required method for Designer support - do not modify
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			this.processInstaller = new ServiceProcessInstaller();
			this.serviceInstaller = new ServiceInstaller();
			this.processInstaller.Account = ServiceAccount.LocalSystem;

			this.serviceInstaller.ServiceName = "LuceneMonitor";
			this.serviceInstaller.StartType = ServiceStartMode.Manual;

			Installers.Add(this.processInstaller);
			Installers.Add(this.serviceInstaller);

		}
		#endregion

		public override void Install(IDictionary stateSaver)
		{
			RegistryKey system;
			RegistryKey currentControlSet;	//HKEY_LOCAL_MACHINE\Services\CurrentControlSet
			RegistryKey services;			//...\Services
			RegistryKey service;			//...\<Service Name>

			try
			{
				//Let the project installer do its job
				base.Install(stateSaver);

				system = Microsoft.Win32.Registry.LocalMachine.OpenSubKey("System");	//Open the HKEY_LOCAL_MACHINE\SYSTEM key
				currentControlSet = system.OpenSubKey("CurrentControlSet");				//Open CurrentControlSet
				services = currentControlSet.OpenSubKey("Services");					//Go to the services key
				service = services.OpenSubKey(this.serviceInstaller.ServiceName, true);	//Open the key for serviceInstaller

				service.SetValue("Description", "Lucene Monitor");


			}
			catch(Exception e)
			{
				Console.WriteLine("An exception was thrown during service installation:\n" + e.ToString());
			}
		}

		public override void Uninstall(IDictionary savedState)
		{
			RegistryKey system;
			RegistryKey currentControlSet;	//HKEY_LOCAL_MACHINE\Services\CurrentControlSet
			RegistryKey services;			//...\Services
			RegistryKey service;			//...\<Service Name>

			try
			{
				//Drill down to the service key and open it with write permission
				system = Registry.LocalMachine.OpenSubKey("System");
				currentControlSet = system.OpenSubKey("CurrentControlSet");
				services = currentControlSet.OpenSubKey("Services");
				service = services.OpenSubKey(this.serviceInstaller.ServiceName, true);
				service.DeleteSubKeyTree("Description");		//Delete keys created during installation

			}
			catch(Exception e)
			{
				Console.WriteLine("Exception encountered while uninstalling service:\n" + e.ToString());
			}
			finally
			{
				//Let the project installer do its job
				base.Uninstall(savedState);
			}
		}


	}
}
