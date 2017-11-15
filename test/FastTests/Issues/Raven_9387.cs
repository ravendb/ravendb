using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Xunit;

namespace FastTests.Issues
{
    public class Raven_9387 : NoDisposalNeeded
    {
        [LinuxFact]
        public void GivenPathStartingWithHomeShouldReplaceThatWithUserDir()
        {
            PathSetting p = new PathSetting("$HOME/raven");
            var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            var fileinfo = new DirectoryInfo(Path.Combine(home, "raven"));
            Assert.Equal(fileinfo.FullName, p.FullPath);
        }
    }
}
