using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;


namespace Raven.Server
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : Installer
    {
        internal const string SERVICE_NAME = "RavenDB";

        public ProjectInstaller()
        {
            InitializeComponent();

            this.serviceInstaller1.DisplayName = SERVICE_NAME;
            this.serviceInstaller1.ServiceName = SERVICE_NAME;
            this.serviceInstaller1.StartType = System.ServiceProcess.ServiceStartMode.Automatic;

            this.serviceProcessInstaller1.Account = System.ServiceProcess.ServiceAccount.LocalSystem;
        }
    }
}
