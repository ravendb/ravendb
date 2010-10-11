using System.ComponentModel;
using System.Configuration.Install;
using System.Diagnostics;
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


			Installers.Add(new PerformanceCounterInstaller
			{
				CategoryName = "RavenDB",
				CategoryType = PerformanceCounterCategoryType.MultiInstance,
				Counters =
					{
						new CounterCreationData("# of tasks / sec",
						                        "Total number of tasks processed per second",
						                        PerformanceCounterType.RateOfCountsPerSecond32)
					}
			});
		}
	}
}
