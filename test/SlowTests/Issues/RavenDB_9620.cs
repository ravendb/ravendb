using System;
using FastTests;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9620:RavenTestBase
    {
        [Fact]
        public void CreateServerWithIllegalAppdrivePath()
        {
            Assert.IsType(typeof(InvalidOperationException),                
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
