using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Embedded;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
// ReSharper disable UseAwaitUsing
// ReSharper disable IdentifierTypo

namespace EmbeddedTests
{
    public class SmugglerTests : EmbeddedTestBase
    {
        [Fact]
        public async Task SmugglerImportFileShouldThrowTimeout()
        {
            var paths = CopyServer();

            using (var embedded = new EmbeddedServer())
            {
                const string fileName = "dump.cmp";
                embedded.StartServer(new ServerOptions
                {
                    ServerDirectory = paths.ServerDirectory,
                    DataDirectory = paths.DataDirectory,
                });

                const string databaseName = "test";
                var dummyDump = CreateDummyDump(1);

                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                using (var bjro = ctx.ReadObject(dummyDump, "dump"))
                using (var ms = File.Create(fileName))
                using (var s = new GZipStream(ms, CompressionMode.Compress))
                {
                    bjro.WriteJsonTo(s);
                }

                using (var store = embedded.GetDocumentStore(new DatabaseOptions(databaseName)))
                {
                    var testingStuff = store.Smuggler.ForTestingPurposesOnly();
                    using (store.SetRequestTimeout(TimeSpan.FromMilliseconds(1000), databaseName))
                    using (testingStuff.CallBeforeSerializeToStreamAsync(() =>
                    {
                        Thread.Sleep(2000);
                    }))
                    {
                        Exception e = null;
                        try
                        {
                            var operation = await store.Smuggler.ForDatabase(databaseName).ImportAsync(new DatabaseSmugglerImportOptions
                            {
                                OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Identities | DatabaseItemType.CompareExchange
                            }, fileName);

                            await operation.WaitForCompletionAsync();
                        }
                        catch (Exception exception)
                        {
                            e = exception;
                        }

                        Assert.NotNull(e);
                        AssertException(e);
                    }
                }
            }
        }

        [Fact]
        public async Task SmugglerImportStreamShouldThrowTimeout()
        {
            var paths = CopyServer();

            using (var embedded = new EmbeddedServer())
            {
                embedded.StartServer(new ServerOptions
                {
                    ServerDirectory = paths.ServerDirectory,
                    DataDirectory = paths.DataDirectory,
                });

                const string databaseName = "test";
                var dummyDump = CreateDummyDump(1);

                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                using (var bjro = ctx.ReadObject(dummyDump, "dump"))
                using (var ms = new MemoryStream())
                using (var zipStream = new GZipStream(ms, CompressionMode.Compress))
                {
                    bjro.WriteJsonTo(zipStream);
                    zipStream.Flush();
                    ms.Position = 0;

                    using (var store = embedded.GetDocumentStore(new DatabaseOptions(databaseName)))
                    {
                        var testingStuff = store.Smuggler.ForTestingPurposesOnly();
                        using (store.SetRequestTimeout(TimeSpan.FromMilliseconds(1000), databaseName))
                        using (testingStuff.CallBeforeSerializeToStreamAsync(() =>
                        {
                            Thread.Sleep(2000);
                        }))
                        {
                            Exception e = null;
                            try
                            {
                                var operation = await store.Smuggler.ForDatabase(databaseName).ImportAsync(new DatabaseSmugglerImportOptions
                                {
                                    OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Identities | DatabaseItemType.CompareExchange
                                }, ms);

                                await operation.WaitForCompletionAsync();
                            }
                            catch (Exception exception)
                            {
                                e = exception;
                            }

                            Assert.NotNull(e);
                            AssertException(e);
                        }
                    }
                }
            }
        }

        private static void AssertException(Exception e)
        {
            if (e is AggregateException ae)
                e = Raven.Client.Extensions.ExceptionExtensions.ExtractSingleInnerException(ae);

            Assert.Equal(typeof(RavenException), e.GetType());
            Assert.NotNull(e.InnerException);
            Assert.Equal(typeof(TimeoutException), e.InnerException.GetType());
        }

        internal static DynamicJsonValue CreateDummyDump(int count)
        {
            var docsList = new List<DynamicJsonValue>();

            for (int i = 0; i < count; i++)
            {
                docsList.Add(new DynamicJsonValue()
                {
                    ["π"] = Math.PI,
                    ["e"] = Math.E,
                    ["Num"] = 0xDEAD,
                    ["@metadata"] = new DynamicJsonValue()
                    {
                        ["@collection"] = $"{nameof(Math)}s",
                        ["@id"] = Guid.NewGuid().ToString()
                    }
                });
            }

            var dummyDump = new DynamicJsonValue()
            {
                ["Docs"] = new DynamicJsonArray(docsList)
            };

            return dummyDump;
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

#if DEBUG
            var runtimeConfigPath = @"../../../../../src/Raven.Server/bin/x64/Debug/net5.0/Raven.Server.runtimeconfig.json";
            if (File.Exists(runtimeConfigPath) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                runtimeConfigPath = @"../../../../../src/Raven.Server/bin/Debug/net5.0/Raven.Server.runtimeconfig.json";
#else
                var runtimeConfigPath = @"../../../../../src/Raven.Server/bin/x64/Release/net5.0/Raven.Server.runtimeconfig.json";
                if (File.Exists(runtimeConfigPath) == false) // this can happen when running directly from CLI e.g. dotnet xunit
                    runtimeConfigPath = @"../../../../../src/Raven.Server/bin/Release/net5.0/Raven.Server.runtimeconfig.json";
#endif

            var runtimeConfigFileInfo = new FileInfo(runtimeConfigPath);
            if (runtimeConfigFileInfo.Exists == false)
                throw new FileNotFoundException("Could not find runtime config", runtimeConfigPath);

            File.Copy(runtimeConfigPath, Path.Combine(serverDirectory, runtimeConfigFileInfo.Name), true);

            foreach (var extension in new[] { "*.dll", "*.so", "*.dylib", "*.deps.json" })
            {
                foreach (var file in Directory.GetFiles(runtimeConfigFileInfo.DirectoryName, extension))
                {
                    var fileInfo = new FileInfo(file);
                    File.Copy(file, Path.Combine(serverDirectory, fileInfo.Name), true);
                }
            }

            var runtimesSource = Path.Combine(runtimeConfigFileInfo.DirectoryName, "runtimes");
            var runtimesDestination = Path.Combine(serverDirectory, "runtimes");

            foreach (string dirPath in Directory.GetDirectories(runtimesSource, "*",
                SearchOption.AllDirectories))
                Directory.CreateDirectory(dirPath.Replace(runtimesSource, runtimesDestination));

            foreach (string newPath in Directory.GetFiles(runtimesSource, "*.*",
                SearchOption.AllDirectories))
                File.Copy(newPath, newPath.Replace(runtimesSource, runtimesDestination), true);

            return (serverDirectory, dataDirectory);
        }
    }
}
