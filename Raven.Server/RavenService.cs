using System.ServiceProcess;

namespace Raven.Server
{
    internal partial class RavenService : ServiceBase
    {
        private DivanServer server;

        public RavenService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            server = new DivanServer(new RavenConfiguration());
        }

        protected override void OnStop()
        {
            if (server != null)
                server.Dispose();
        }
    }
}