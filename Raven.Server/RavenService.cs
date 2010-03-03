using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;

namespace Raven.Server
{
    partial class RavenService : ServiceBase
    {
        DivanServer server;

        public RavenService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            server = new DivanServer(@"..\..\..\Data", 8080);
        }

        protected override void OnStop()
        {
            if (server != null)
                server.Dispose();
        }
    }
}
