using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Management.Automation;
using System.ComponentModel;
using System.Management.Automation.Runspaces;
using System.Collections.ObjectModel;

namespace Raven.Server.PowerShellProvider
{
    [RunInstaller(true)]
    public class RavenDBSnapIn : PSSnapIn
    {
        public override string Description
        {
            get { return "RavenDB Powershell Provider"; }
        }
        public override string Name
        {
            get { return "RavenDB"; }
        }
        public override string Vendor
        {
            get { return "Hibernating Rhinos"; }
        }

        //public override Collection<ProviderConfigurationEntry> Providers
        //{
        //    get
        //    {
        //        return new Collection<ProviderConfigurationEntry>()
        //        {
        //            new ProviderConfigurationEntry("RavenDB", typeof(RavenDBProvider), "RavenDBProvider.help.xml")
        //        };
        //    }
        //}

        //public override Collection<CmdletConfigurationEntry> Cmdlets
        //{
        //    get
        //    {
        //        return new Collection<CmdletConfigurationEntry>()
        //        {
        //            new CmdletConfigurationEntry("RavenDB", typeof(RavenDBProvider), "RavenDBProvider.help.xml")
        //        };
        //    }
        //}
    }
}
