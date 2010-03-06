using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Management.Automation;

namespace Raven.Server.PowerShellProvider
{
    internal class RavenDBPSDriveInfo : PSDriveInfo
    {
        public RavenDBPSDriveInfo(PSDriveInfo driveInfo) : base(driveInfo) { }

        public Database.DocumentDatabase Database { get; set; }
    }
}
