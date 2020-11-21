using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
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

        private class Person
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }
    }
}
