using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Embedded;
using Xunit;

namespace EmbeddedTests
{
    public class BasicTests : EmbeddedTestBase
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
                });

                using (var store = embedded.GetDocumentStore(new DatabaseOptions("Test")
                {
                    Conventions = new DocumentConventions
                    {
                        SaveEnumsAsIntegers = true
                    }
                }))
                {
                    Assert.True(store.Conventions.SaveEnumsAsIntegers);
                    Assert.True(store.GetRequestExecutor().Conventions.SaveEnumsAsIntegers);
                    Assert.True(store.Conventions.DisableTopologyCache);

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
                });

                using (var store = embedded.GetDocumentStore("Test"))
                {
                    Assert.False(store.Conventions.SaveEnumsAsIntegers);
                    Assert.False(store.GetRequestExecutor().Conventions.SaveEnumsAsIntegers);
                    Assert.True(store.Conventions.DisableTopologyCache);

                    using (var session = store.OpenSession())
                    {
                        var person = session.Load<Person>("people/1");

                        Assert.NotNull(person);
                        Assert.Equal("John", person.Name);
                    }
                }
            }
        }

        [Fact]
        public async Task TestEmbeddedRestart()
        {
            var paths = CopyServer();

            using (var embedded = new EmbeddedServer())
            {
                embedded.StartServer(new ServerOptions
                {
                    ServerDirectory = paths.ServerDirectory,
                    DataDirectory = paths.DataDirectory,
                });

                var pid1 = await embedded.GetServerProcessIdAsync();
                Assert.True(pid1 > 0);

                var mre = new ManualResetEventSlim();

                embedded.ServerProcessExited += (s, args) => mre.Set();

                using (var process = Process.GetProcessById(pid1))
                {
                    try
                    {
                        process.Kill();
                    }
                    catch
                    {
                        // ignored
                    }
                }

                Assert.True(mre.Wait(TimeSpan.FromSeconds(15)));

                await embedded.RestartServerAsync();

                var pid2 = await embedded.GetServerProcessIdAsync();
                Assert.True(pid2 > 0);
                Assert.NotEqual(pid1, pid2);

                mre.Reset();
                await embedded.RestartServerAsync();

                Assert.True(mre.Wait(TimeSpan.FromSeconds(15)));

                var pid3 = await embedded.GetServerProcessIdAsync();
                Assert.True(pid3 > 0);
                Assert.NotEqual(pid2, pid3);
            }
        }

        [Fact]
        public async Task TestEmbedded_RuntimeFrameworkVersionMatcher()
        {
            var paths = CopyServer();

            using (var embedded = new EmbeddedServer())
            {
                var options = new ServerOptions
                {
                    ServerDirectory = paths.ServerDirectory,
                    DataDirectory = paths.DataDirectory,
                };

                var frameworkVersion = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion(options.FrameworkVersion)
                {
                    Patch = null
                };

                options.FrameworkVersion = frameworkVersion.ToString();

                embedded.StartServer(options);

                var pid1 = await embedded.GetServerProcessIdAsync();
                Assert.True(pid1 > 0);
            }
        }

        [Fact]
        public async Task TcpCompressionOnSubscriptionShouldNotWorkInNetStandard2()
        {
            var file = Path.GetTempFileName();
            try
            {
                var paths = CopyServer();
                using (var embedded = new EmbeddedServer())
                {
                    var options = new ServerOptions
                    {
                        ServerDirectory = paths.ServerDirectory,
                        DataDirectory = paths.DataDirectory
                    };
                    embedded.StartServer(options);

                    using (var store1 = embedded.GetDocumentStore(new DatabaseOptions("Test-1")))
                    using (var store2 = embedded.GetDocumentStore(new DatabaseOptions("Test-2")))
                    {
                        store1.Subscriptions.Create(new SubscriptionCreationOptions<Person>() { Name = "sub1" });
                        store1.Subscriptions.Create(new SubscriptionCreationOptions<Person>() { Name = "sub2" });
                        store1.Subscriptions.Create(new SubscriptionCreationOptions<Person>());

                        var subscriptionStataList = store1.Subscriptions.GetSubscriptions(0, 10);

                        Assert.Equal(3, subscriptionStataList.Count);

                        var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        subscriptionStataList = store2.Subscriptions.GetSubscriptions(0, 10, store2.Database);

                        Assert.Equal(3, subscriptionStataList.Count);
                        Assert.True(subscriptionStataList.Any(x => x.SubscriptionName.Equals("sub1")));
                        Assert.True(subscriptionStataList.Any(x => x.SubscriptionName.Equals("sub2")));
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        private class Person
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }
    }
}
