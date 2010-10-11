using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Extensions;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class HiLoServerKeysNotExported : IDisposable
    {
        private DocumentStore documentStore;
        private RavenDbServer server;

        public HiLoServerKeysNotExported()
        {
            CreateServer(true);


        }

        private void CreateServer(bool initDocStore = false)
        {
            IOExtensions.DeleteDirectory("HiLoData");
            server = new RavenDbServer(new RavenConfiguration { Port = 8080, DataDirectory = "HiLoData", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true });

            if (initDocStore) {
                documentStore = new DocumentStore() { Url = "http://localhost:8080/" };
                documentStore.Initialize();
            }
        }

        [Fact]
        public void Export_And_Import_Retains_HiLoState()
        {
            using (var session = documentStore.OpenSession()) {
                var foo = new Foo() { Something = "something2" };
                Assert.Null(foo.Id);
                session.Store(foo);
                Assert.NotNull(foo.Id);
                session.SaveChanges();
            }

            if (File.Exists("hilo-export.dump"))
				File.Delete("hilo-export.dump");
			Smuggler.Smuggler.ExportData("http://localhost:8080/", "hilo-export.dump", false);
			Assert.True(File.Exists("hilo-export.dump"));

            using (var session = documentStore.OpenSession()) {
                var hilo = session.Load<HiLoKey>("Raven/Hilo/foos");
                Assert.NotNull(hilo);
                Assert.Equal(2, hilo.ServerHi);
            }

            server.Dispose();
            CreateServer();

			Smuggler.Smuggler.ImportData("http://localhost:8080/", "hilo-export.dump");

            using (var session = documentStore.OpenSession()) {
                var hilo = session.Load<HiLoKey>("Raven/Hilo/foos");
                Assert.NotNull(hilo);
				Assert.Equal(2, hilo.ServerHi);
			}
        }

        public class Foo
        {
            public string Id { get; set; }
            public string Something { get; set; }
        }

        private class HiLoKey
        {
            public long ServerHi { get; set; }

        }

        public void Dispose()
        {
            documentStore.Dispose();
            server.Dispose();
            IOExtensions.DeleteDirectory("HiLoData");
        }

    }
}
