extern alias database;
using System;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Reflection;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Bundles.Expiration;
using Raven.Client.Document;
using Raven.Database;
using Raven.Server;
using Xunit;
using System.Linq;

namespace Raven.Bundles.Tests.Expiration
{
    public class Expiration : IDisposable
    {
        private readonly DocumentStore documentStore;
        private readonly string path;
        private readonly RavenDbServer ravenDbServer;

        public Expiration()
        {
            path = Path.GetDirectoryName(Assembly.GetAssembly(typeof (Versioning)).CodeBase);
            path = Path.Combine(path, "TestDb").Substring(6);
            if (Directory.Exists(path))
                Directory.Delete(path, true);
            ravenDbServer = new RavenDbServer(
                new database::Raven.Database.RavenConfiguration
                {
                    Port = 58080,
                    RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
                    DataDirectory = path,
                    Catalog =
                        {
                            Catalogs =
                                {
                                    new AssemblyCatalog(typeof (ExpirationReadTrigger).Assembly)
                                }
                        },
                });
            ExpirationReadTrigger.GetCurrentUtcDate = () => DateTime.UtcNow;
            documentStore = new DocumentStore
            {
                Url = "http://localhost:58080"
            };
            documentStore.Initialize();
        }

        #region IDisposable Members

        public void Dispose()
        {
            documentStore.Dispose();
            ravenDbServer.Dispose();
            if (Directory.Exists(path))
                Directory.Delete(path, true);
        }

        #endregion

        [Fact]
        public void Can_add_entity_with_expiry_then_read_it_before_it_expires()
        {
            var company = new Company {Name = "Company Name"};
            var expiry = DateTime.UtcNow.AddMinutes(5);
            using (var session = documentStore.OpenSession())
            {
                session.Store(company);
                session.GetMetadataFor(company)["Raven-Expiration-Date"] = new JValue(expiry);
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                var company2 = session.Load<Company>(company.Id);
                Assert.NotNull(company2);
                var metadata = session.GetMetadataFor(company2);
                var dateAsJsStr = @"\/Date("+(long)( expiry - new DateTime(1970,1,1) ).TotalMilliseconds+@")\/";
                Assert.Equal(dateAsJsStr, metadata.Value<string>("Raven-Expiration-Date"));
            }
        }

        [Fact]
        public void Can_add_entity_with_expiry_but_will_not_be_able_to_read_it_after_expiry()
        {
            var company = new Company { Name = "Company Name" };
            var expiry = DateTime.UtcNow.AddMinutes(5);
            using (var session = documentStore.OpenSession())
            {
                session.Store(company);
                session.GetMetadataFor(company)["Raven-Expiration-Date"] = new JValue(expiry);
                session.SaveChanges();
            }
            ExpirationReadTrigger.GetCurrentUtcDate = () => DateTime.UtcNow.AddMinutes(10);
           
            using (var session = documentStore.OpenSession())
            {
                var company2 = session.Load<Company>(company.Id);
                Assert.Null(company2);
            }
        }

        [Fact]
        public void After_expiry_passed_document_will_be_physically_deleted()
        {
            var company = new Company
            {
                Id = "companies/1",
                Name = "Company Name"
            };
            var expiry = DateTime.UtcNow.AddMinutes(5);
            using (var session = documentStore.OpenSession())
            {
                session.Store(company);
                session.GetMetadataFor(company)["Raven-Expiration-Date"] = new JValue(expiry);
                session.SaveChanges();

                session.LuceneQuery<Company>("Raven/DocumentsByExpirationDate")
                    .WaitForNonStaleResults()
                    .ToList();
            }
            ExpirationReadTrigger.GetCurrentUtcDate = () => DateTime.UtcNow.AddMinutes(10);

            using (var session = documentStore.OpenSession())
            {
                session.Store(new Company
                {
                    Id = "companies/2",
                    Name = "Company Name"
                });
                session.SaveChanges(); // this forces the background task to run
            }

            database::Raven.Database.JsonDocument documentByKey = null;
            for (int i = 0; i < 15; i++)
            {
                ravenDbServer.Database.TransactionalStorage.Batch(accessor =>
                {
                    documentByKey = accessor.Documents.DocumentByKey("companies/1", null);
                });
                if (documentByKey == null)
                    return;
                Thread.Sleep(100);
            }
            Assert.False(true, "Document was not deleted");
        }
    }
}