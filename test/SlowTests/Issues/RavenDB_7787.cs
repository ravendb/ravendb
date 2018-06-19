using System;
using System.IO;
using FastTests;
using Raven.Embedded;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7787 : RavenTestBase
    {
        [Fact]
        public void TestEmbedded()
        {
            var paths = CopyServer();

            using (var embedded = new EmbeddedServer())
            {
                embedded.StartServer(new ServerOptions
                {
                    ServerDirectory = paths.ServerDirectory,
                    DataDirectory = paths.DataDirectory,
                    FrameworkVersion = null
                });

                using (var store = embedded.GetDocumentStore("Test"))
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Person
                        {
                            Name = "John"
                        }, "people/1");

                        session.SaveChanges();
                    }
                }
            }

            using (var embedded = new EmbeddedServer())
            {
                embedded.StartServer(new ServerOptions
                {
                    ServerDirectory = paths.ServerDirectory,
                    DataDirectory = paths.DataDirectory,
                    FrameworkVersion = null
                });

                using (var store = embedded.GetDocumentStore("Test"))
                {
                    using (var session = store.OpenSession())
                    {
                        var person = session.Load<Person>("people/1");

                        Assert.NotNull(person);
                        Assert.Equal("John", person.Name);
                    }
                }
            }
        }

        private (string ServerDirectory, string DataDirectory) CopyServer()
        {
            var baseDirectory = NewDataPath();
            var serverDirectory = Path.Combine(baseDirectory, "RavenDBServer");
            var dataDirectory = Path.Combine(baseDirectory, "RavenDB");

            if (Directory.Exists(serverDirectory) == false)
                Directory.CreateDirectory(serverDirectory);

            if (Directory.Exists(dataDirectory) == false)
                Directory.CreateDirectory(dataDirectory);

            foreach (var file in Directory.GetFiles(AppContext.BaseDirectory, "*.dll"))
            {
                var fileInfo = new FileInfo(file);
                File.Copy(file, Path.Combine(serverDirectory, fileInfo.Name));
            }

#if DEBUG
            var runtimeConfigPath = @"../../../../../src/Raven.Server/bin/x64/Debug/netcoreapp2.1/Raven.Server.runtimeconfig.json";
            if (File.Exists(runtimeConfigPath) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                runtimeConfigPath = @"../../../../../src/Raven.Server/bin/Debug/netcoreapp2.1/Raven.Server.runtimeconfig.json";
#else
                var runtimeConfigPath = @"../../../../../src/Raven.Server/bin/x64/Release/netcoreapp2.1/Raven.Server.runtimeconfig.json";
                if (File.Exists(runtimeConfigPath) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                    runtimeConfigPath = @"../../../../../src/Raven.Server/bin/Release/netcoreapp2.1/Raven.Server.runtimeconfig.json";
#endif

            var runtimeConfigFileInfo = new FileInfo(runtimeConfigPath);
            if (runtimeConfigFileInfo.Exists == false)
                throw new FileNotFoundException("Could not find runtime config", runtimeConfigPath);

            File.Copy(runtimeConfigPath, Path.Combine(serverDirectory, runtimeConfigFileInfo.Name));

            return (serverDirectory, dataDirectory);
        }
    }
}
