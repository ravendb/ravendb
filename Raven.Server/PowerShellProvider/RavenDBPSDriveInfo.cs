using System.Management.Automation;
using Raven.Database;

namespace Raven.Server.PowerShellProvider
{
    internal class RavenDBPSDriveInfo : PSDriveInfo
    {
        public RavenDBPSDriveInfo(PSDriveInfo driveInfo) : base(driveInfo)
        {
        }

        public DocumentDatabase Database { get; set; }
    }
}