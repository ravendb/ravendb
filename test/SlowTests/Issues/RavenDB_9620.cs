using System;
using FastTests;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9620:RavenTestBase
    {
        public RavenDB_9620(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CreateServerWithIllegalAppdrivePath()
        {
            Assert.IsType(typeof(ArgumentException),
                Assert.Throws<Raven.Client.Exceptions.RavenException>(() => 
                GetDocumentStore(new Options
                {
                    ModifyDatabaseRecord = x =>
                    {
                        x.Settings[
                            RavenConfiguration.GetKey(config => config.Core.DataDirectory)] = "APPDRIVE:";
                    }
                })).InnerException);            
        }
    }
}
