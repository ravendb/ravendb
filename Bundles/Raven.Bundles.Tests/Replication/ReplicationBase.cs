extern alias replication;

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using log4net.Appender;
using log4net.Config;
using log4net.Filter;
using log4net.Layout;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database;
using Raven.Server;

namespace Raven.Bundles.Tests.Replication
{
    public class ReplicationBase : IDisposable
    {
        private readonly List<IDocumentStore> stores = new List<IDocumentStore>();
        protected readonly List<RavenDbServer> servers = new List<RavenDbServer>();

        public ReplicationBase()
        {
            for (int i = 0; i < 15; i++)
            {
                if(Directory.Exists("Data #" + i))
                    Directory.Delete("Data #" + i, true);
            }

            var outputDebugStringAppender = new OutputDebugStringAppender
            {
                Layout = new SimpleLayout(),
            };
            outputDebugStringAppender.AddFilter(new LoggerMatchFilter
            {
                AcceptOnMatch = true,
                LoggerToMatch = "Raven.Bundles"
            });
            outputDebugStringAppender.AddFilter(new DenyAllFilter());
            BasicConfigurator.Configure(outputDebugStringAppender);
        }

        private const int PortRangeStart = 9101;
        protected const int RetriesCount = 15;

        public IDocumentStore CreateStore()
        {
            var port = PortRangeStart + servers.Count;
            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(port);
            var ravenDbServer = new RavenDbServer(new RavenConfiguration
            {
                AnonymousUserAccessMode = AnonymousUserAccessMode.All,
                Catalog = {Catalogs = {new AssemblyCatalog(typeof (replication::Raven.Bundles.Replication.Triggers.AncestryPutTrigger).Assembly)}},
                DataDirectory = "Data #" + servers.Count,
                RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
                Port = port
            });
            servers.Add(ravenDbServer);
            var documentStore = new DocumentStore{Url = ravenDbServer.Database.Configuration.ServerUrl};
            documentStore.Initialise();
            stores.Add(documentStore);
            return documentStore;
        }

        public void Dispose()
        {
            foreach (var documentStore in stores)
            {
                documentStore.Dispose();
            }

            foreach (var ravenDbServer in servers)
            {
                ravenDbServer.Dispose();
                try
                {
                    Directory.Delete(ravenDbServer.Database.Configuration.DataDirectory);
                }
                catch (Exception)
                {
                }
            }
        }

        protected void TellFirstInstanceToReplicateToSecondInstance()
        {
            TellInstanceToReplicateToAnotherInstance(0, 1);
        }

        protected void TellSecondInstanceToReplicateToFirstInstance()
        {
            TellInstanceToReplicateToAnotherInstance(1, 0);
        }

        protected void TellInstanceToReplicateToAnotherInstance(int src, int dest)
        {
            using (var session = stores[src].OpenSession())
            {
                session.Store(new replication::Raven.Bundles.Replication.Data.ReplicationDocument
                {
                    Destinations = {new replication::Raven.Bundles.Replication.Data.ReplicationDestination
                    {
                        Url = servers[dest].Database.Configuration.ServerUrl
                    }}
                });
                session.SaveChanges();
            }
        }
    }
}