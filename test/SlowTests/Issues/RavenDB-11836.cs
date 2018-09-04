using System.Collections.Generic;
using System.IO;
using FastTests;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11836 : RavenTestBase
    {
        [Fact]
        public void ShouldThrowIfTryingToCreateDatabaseWithSamePathAsServerDataDir()
        {
            using (var store = GetDocumentStore())
            {
                var dbRecord = new DatabaseRecord("test")
                {
                    Settings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = Server.Configuration.Core.DataDirectory.ToString()
                    }
                };

                var e = Assert.Throws<BadRequestException>(() =>
                {
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(dbRecord));
                });

                Assert.Contains($"Forbidden data directory path for database 'test': '{Server.Configuration.Core.DataDirectory}'. This is the root path that RavenDB server uses to store data.", e.Message);
            }
        }

        [Fact]
        public void ShouldThrowIfTryingToCreateDatabaseWithRootDirectoryAsPath()
        {
            using (var store = GetDocumentStore())
            {
                var root = Path.GetPathRoot(Server.Configuration.Core.DataDirectory.FullPath);

                var dbRecord = new DatabaseRecord("test")
                {
                    Settings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = root
                    }
                };

                var e = Assert.Throws<BadRequestException>(() =>
                {
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(dbRecord));
                });

                Assert.Contains($"Forbidden data directory path for database 'test': '{root}'. You cannot use the root directory of the drive as the database path.", e.Message);
            }
        }
    }
}
