using System;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Reflection;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Tests;
using Raven.Client.Tests.Document;
using Xunit;

namespace Raven.Bundles.Tests
{
    public class Versioning : BaseTest, IDisposable
    {
        private string path;

        #region IDisposable Members

        public void Dispose()
        {
            Directory.Delete(path, true);
        }

        #endregion

        private DocumentStore NewDocumentStore()
        {
            path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(Versioning)).CodeBase);
            path = Path.Combine(path, "TestDb").Substring(6);
            var documentStore = new DocumentStore
            {
                Configuration =
                    {
                        DataDirectory = path,
                        RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
                        Catalog =
                            {
                                Catalogs =
                                    {
                                        new AssemblyCatalog(typeof (Bundles.Versioning.PutTrigger).Assembly)
                                    }
                            }
                    }
            };
            documentStore.Initialise();
            return documentStore;
        }

        [Fact]
        public void Will_automatically_set_metadata()
        {
            using (var documentStore = NewDocumentStore())
            {
                var company = new Company { Name = "Company Name" };
                using(var session = documentStore.OpenSession())
                {
                    session.Store(company);
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var company2 = session.Load<Company>(company.Id);
                    var metadata = session.GetMetadataFor(company2);
                    Assert.Equal("Current", metadata.Value<string>("Raven-Document-Revision-Status"));
                    Assert.Equal(1, metadata.Value<int>("Raven-Document-Revision"));
                }
            }
        }

        [Fact]
        public void Will_automatically_update_metadata_on_next_insert()
        {
            using (var documentStore = NewDocumentStore())
            {
                var company = new Company { Name = "Company Name" };
                using (var session = documentStore.OpenSession())
                {
                    session.Store(company);
                    session.SaveChanges();
                    company.Name = "Hibernating Rhinos";
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var company2 = session.Load<Company>(company.Id);
                    var metadata = session.GetMetadataFor(company2);
                    Assert.Equal("Current", metadata.Value<string>("Raven-Document-Revision-Status"));
                    Assert.Equal(2, metadata.Value<int>("Raven-Document-Revision"));
                }
            }
        }

        [Fact]
        public void Will_automatically_craete_duplicate_on_first_insert()
        {
            using (var documentStore = NewDocumentStore())
            {
                var company = new Company { Name = "Company Name" };
                using (var session = documentStore.OpenSession())
                {
                    session.Store(company);
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var company2 = session.Load<Company>(company.Id + "/revisions/1");
                    var metadata = session.GetMetadataFor(company2);
                    Assert.Equal(company.Name, company2.Name);
                    Assert.Equal("Historical", metadata.Value<string>("Raven-Document-Revision-Status"));
                    Assert.Equal(1, metadata.Value<int>("Raven-Document-Revision"));
                }
            }
        }

        [Fact]
        public void Will_automatically_craete_duplicate_on_next_insert()
        {
            using (var documentStore = NewDocumentStore())
            {
                var company = new Company { Name = "Company Name" };
                using (var session = documentStore.OpenSession())
                {
                    session.Store(company);
                    session.SaveChanges();
                    company.Name = "Hibernating Rhinos";
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var company2 = session.Load<Company>(company.Id + "/revisions/1");
                    var metadata = session.GetMetadataFor(company2);
                    Assert.Equal("Company Name", company2.Name);
                    Assert.Equal("Historical", metadata.Value<string>("Raven-Document-Revision-Status"));
                    Assert.Equal(1, metadata.Value<int>("Raven-Document-Revision"));

                    company2 = session.Load<Company>(company.Id + "/revisions/2");
                    metadata = session.GetMetadataFor(company2);
                    Assert.Equal("Hibernating Rhinos", company2.Name);
                    Assert.Equal("Historical", metadata.Value<string>("Raven-Document-Revision-Status"));
                    Assert.Equal(2, metadata.Value<int>("Raven-Document-Revision"));
                }
            }
        }
    }
}
