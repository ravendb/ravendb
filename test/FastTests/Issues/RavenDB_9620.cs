using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.Config;
using Xunit;

namespace FastTests.Issues
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
