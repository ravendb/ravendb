using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

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
			this.serviceInstaller1.StartType = ServiceStartMode.Automatic;

			this.serviceProcessInstaller1.Account = ServiceAccount.LocalSystem;
		}
	}
}