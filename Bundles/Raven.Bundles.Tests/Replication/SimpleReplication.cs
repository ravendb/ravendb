using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Threading;
using log4net.Appender;
using log4net.Config;
using log4net.Filter;
using log4net.Layout;
using Raven.Bundles.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database;
using Raven.Server;
using Xunit;

namespace Raven.Bundles.Tests.Replication
{
    public class SimpleReplication : IDisposable
    {
        private const int PortRangeStart = 9101;

        private readonly List<IDocumentStore> stores = new List<IDocumentStore>();
        private readonly List<RavenDbServer> servers = new List<RavenDbServer>();
        private int RetriesCount = 150;

        public SimpleReplication()
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

        public IDocumentStore CreateStore()
        {
            var port = PortRangeStart + servers.Count;
            RavenDbServer.EnsureCanListenToWhenInNonAdminContext(port);
            var ravenDbServer = new RavenDbServer(new RavenConfiguration
            {
                AnonymousUserAccessMode = AnonymousUserAccessMode.All,
                Catalog = {Catalogs = {new AssemblyCatalog(typeof (AncestryPutTrigger).Assembly)}},
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

        [Fact]
        public void Can_replicate_between_two_instances()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            using (var session = store1.OpenSession())
            {
                session.Store(new ReplicationDocument()
                {
                    Destinations = {new ReplicationDestination
                    {
                        Url = servers[1].Database.Configuration.ServerUrl
                    }}
                });
                session.SaveChanges();
            }

            using(var session = store1.OpenSession())
            {
                session.Store(new Company{Name = "Hibernating Rhinos"});
                session.SaveChanges();
            }


            using(var session = store2.OpenSession())
            {
                Company company = null;
                for (int i = 0; i < RetriesCount; i++)
                {
                    company = session.Load<Company>("companies/1");
                    if (company != null)
                        break;
                    Thread.Sleep(100);
                }
                Assert.NotNull(company);
                Assert.Equal("Hibernating Rhinos",company.Name);
            }
        }

        [Fact]
        public void Will_not_replicate_replicated_documents()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            using (var session = store1.OpenSession())
            {
                session.Store(new ReplicationDocument()
                {
                    Destinations =
                                  {
                                      new ReplicationDestination
                                      {
                                          Url = servers[1].Database.Configuration.ServerUrl
                                      }
                                  }
                });
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Store(new ReplicationDocument()
                {
                    Destinations =
                                  {
                                      new ReplicationDestination
                                      {
                                          Url = servers[0].Database.Configuration.ServerUrl
                                      }
                                  }
                });
                session.SaveChanges();
            }

            string etag;
            string id;
            using (var session = store1.OpenSession())
            {
                var company = new Company {Name = "Hibernating Rhinos"};
                session.Store(company);
                session.SaveChanges();
                id = company.Id;
                session.Clear();
                company = session.Load<Company>(id);
                etag = session.GetMetadataFor(company).Value<string>("@etag");
            }


            using (var session = store2.OpenSession()) // waiting for document to show up.
            {
                Company company = null;
                for (int i = 0; i < RetriesCount; i++)
                {
                    company = session.Load<Company>(id);
                    if (company != null)
                        break;
                    Thread.Sleep(100);
                }
                Assert.NotNull(company);
                Assert.Equal("Hibernating Rhinos", company.Name);
            }

            // assert that the etag haven't changed (we haven't replicated)
            for (int i = 0; i < 15; i++)
            {
                using (var session = store1.OpenSession())
                {
                    Company company = session.Load<Company>(id);
                    Assert.Equal(etag, session.GetMetadataFor(company).Value<string>("@etag"));
                }
                Thread.Sleep(100);
            }
        }

        [Fact]
        public void Can_replicate_delete_between_two_instances()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            using (var session = store1.OpenSession())
            {
                session.Store(new ReplicationDocument()
                {
                    Destinations =
                                  {
                                      new ReplicationDestination
                                      {
                                          Url = servers[1].Database.Configuration.ServerUrl
                                      }
                                  }
                });
                session.SaveChanges();
            }

            using (var session = store1.OpenSession())
            {
                session.Store(new Company {Name = "Hibernating Rhinos"});
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                Company company = null;
                for (int i = 0; i < RetriesCount; i++)
                {
                    company = session.Load<Company>("companies/1");
                    if (company != null)
                        break;
                    Thread.Sleep(100);
                }
                Assert.Equal("Hibernating Rhinos", company.Name);
            }

            using (var session = store1.OpenSession())
            {
                session.Delete(session.Load<Company>("companies/1"));
                session.SaveChanges();
            }
            

            Company deletedCompany = null;
            for (int i = 0; i < RetriesCount; i++)
            {
                using (var session = store2.OpenSession())
                    deletedCompany = session.Load<Company>("companies/1");
                if (deletedCompany == null)
                    break;
                Thread.Sleep(100);
            }
            Assert.Null(deletedCompany);
        }
    }
}