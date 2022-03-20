// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3659.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3659 : RavenTestBase
    {
        private readonly Dictionary<string, string> _invalidCustomSettings;

        public RavenDB_3659(ITestOutputHelper output) : base(output)
        {
            if (LinuxTestUtils.RunningOnPosix)
                _invalidCustomSettings = new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Storage.TempPath), "/proc/no/such/dir"} // in linux for invalid dir we need non permission one like /proc (root / dir can be valid in some cases)
                };
            else
                _invalidCustomSettings = new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Storage.TempPath), "V:\\"}
                };
        }

        [Fact]
        public void IfTempPathCannotBeAccessedThenServerShouldThrowDuringStartup()
        {
            var e = Assert.Throws<InvalidOperationException>(() => UseNewLocalServer(_invalidCustomSettings, runInMemory: false));

            string expectedSubstring = $"Key: '{RavenConfiguration.GetKey(x => x.Storage.TempPath)}' Path: '{_invalidCustomSettings[RavenConfiguration.GetKey(x => x.Storage.TempPath)]}";
            Assert.Contains(expectedSubstring, e.Message);
        }

        [Fact]
        public async Task TenantDatabasesShouldInheritTempPathIfNoneSpecified()
        {
            var tempPath = NewDataPath();

            UseNewLocalServer(new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Storage.TempPath), tempPath }
            }, runInMemory: false);

            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                Assert.Equal(Path.Combine(tempPath, "Databases", store.Database), database.Configuration.Storage.TempPath.FullPath);
            }
        }

        [Fact]
        public async Task TenantDatabasesCanHaveDifferentTempPathSpecified()
        {
            var tempPath1 = NewDataPath();
            var tempPath2 = NewDataPath();

            UseNewLocalServer(new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Storage.TempPath), tempPath1 }
            }, runInMemory: false);

            using (var store = GetDocumentStore())
            {
                store
                    .Maintenance
                    .Server
                    .Send(new CreateDatabaseOperation(new DatabaseRecord("DB1")
                    {
                        Settings = new Dictionary<string, string>
                        {
                            {RavenConfiguration.GetKey(x => x.Storage.TempPath), tempPath2}
                        }
                    }));

                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("DB1");

                Assert.Equal(tempPath2, database.Configuration.Storage.TempPath.FullPath);
            }
        }
    }
}
