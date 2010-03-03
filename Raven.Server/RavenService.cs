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
        DivanServer m_Server = null;

        public RavenService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            m_Server = new DivanServer(@"..\..\..\Data", 8080);
        }

        protected override void OnStop()
        {
            if (m_Server != null)
                m_Server.Dispose();
        }
    }
}
