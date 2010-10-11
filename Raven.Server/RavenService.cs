using System.ServiceProcess;
using Raven.Database;

namespace Raven.Server
{
	internal partial class RavenService : ServiceBase
	{
		private RavenDbServer server;

		public RavenService()
		{
			InitializeComponent();
		}

		protected override void OnStart(string[] args)
		{
			server = new RavenDbServer(new RavenConfiguration());
		}

		protected override void OnStop()
		{
			if (server != null)
				server.Dispose();
		}
	}
}
