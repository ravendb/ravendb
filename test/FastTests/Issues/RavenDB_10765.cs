using System;
using Raven.Server.Config.Settings;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_10765 : NoDisposalNeeded
    {
        [LinuxTheory]
        [InlineData("appdrive:raven")]
        [InlineData("~/raven")]
        [InlineData("$HOME/raven")]
        public void GivenPathStartingWithIllegalKeywordShouldThrowArgumentException(string path)
        {
            try
            {
                new PathSetting(path);
                Assert.True(false, "Should have thrown ArgumentException");
            }
            catch (ArgumentException)
            {
            }
        }
    }
}
