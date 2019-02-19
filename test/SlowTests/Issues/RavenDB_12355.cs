using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Sparrow.Platform;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12355 : RavenTestBase
    {
        [Fact]
        public void Should_Limit_Database_Relative_Path()
        {
            var dataPath = NewDataPath();

            UseNewLocalServer(runInMemory: false, customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = dataPath,
                [RavenConfiguration.GetKey(x => x.Core.EnforceDataDirectoryPath)] = "true"
            });

            var options = new Options
            {
                Path = "../Karmelush"
            };
        
            var e = Assert.Throws<RavenException>(() => GetDocumentStore(options));

            Assert.True(e.InnerException is ArgumentException);
            
        }

        [Fact]
        public void Should_Limit_Database_Absolute_Path()
        {
            var dataPath = NewDataPath();

            UseNewLocalServer(runInMemory: false, customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = dataPath,
                [RavenConfiguration.GetKey(x => x.Core.EnforceDataDirectoryPath)] = "true"
            });

            var options = new Options
            {
                Path = PlatformDetails.RunningOnPosix ? "/home" : "c:\\"
            };

            var e = Assert.Throws<RavenException>(() => GetDocumentStore(options));

            Assert.True(e.InnerException is ArgumentException);

        }

        [Fact]
        public void Should_Not_Limit_Database_Path()
        {
            var dataPath = NewDataPath();

            UseNewLocalServer(runInMemory: false, customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = dataPath,
                [RavenConfiguration.GetKey(x => x.Core.EnforceDataDirectoryPath)] = "true"
            });

            var options = new Options
            {
                Path = "Karmelush"
            };
            GetDocumentStore(options);
        }
    }
}

