//-----------------------------------------------------------------------
// <copyright file="ProjectInstaller.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.ComponentModel;
using System.Configuration.Install;
using System.Diagnostics;
using System.ServiceProcess;

namespace Raven.Server
{
	[RunInstaller(true)]
	public partial class ProjectInstaller : Installer
	{
		internal static string SERVICE_NAME = "RavenDB";

		public ProjectInstaller()
		{
			InitializeComponent();

			ServiceName = SERVICE_NAME;

		    this.serviceInstaller1.Description = "2nd generation document database from Hibernating Rhinos";

			this.serviceInstaller1.StartType = ServiceStartMode.Automatic;

			this.serviceProcessInstaller1.Account = ServiceAccount.LocalSystem;
		}

		public string ServiceName
		{
			get
			{
				return serviceInstaller1.DisplayName;
			}
			set
			{
				serviceInstaller1.DisplayName = value;
				serviceInstaller1.ServiceName = value;
			}
		}
	}
}