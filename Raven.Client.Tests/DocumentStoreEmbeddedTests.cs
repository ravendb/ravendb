using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using Xunit;

namespace Raven.Client.Tests
{
    public class DocumentStoreEmbeddedTests : BaseTest, IDisposable
    {
        private string path;

        private DocumentStore newDocumentStore()
        {
            path = Path.GetDirectoryName(Assembly.GetAssembly(typeof (DocumentStoreServerTests)).CodeBase);
            path = Path.Combine(path, "TestDb").Substring(6);
            var documentStore = new DocumentStore();
            documentStore.DataDirectory = path;
            documentStore.Conventions.FindIdentityProperty = q => q.Name == "Id";
            documentStore.Initialise();
            return documentStore;
        }

        [Fact]
        public void Should_Load_entity_back_with_document_Id_mapped_to_Id()
        {
            using (var documentStore = newDocumentStore())
            {
                var company = new Company { Name = "Company Name" };
                var session = documentStore.OpenSession();
                session.Store(company);

                var companyFound = session.Load<Company>(company.Id);

                Assert.Equal(companyFound.Id, company.Id);
            }
        }

        [Fact]
        public void Should_map_Entity_Id_to_document_during_store()
        {
            using (var documentStore = newDocumentStore())
            {
                var session = documentStore.OpenSession();
                var company = new Company { Name = "Company 1" };
                session.Store(company);
                Assert.NotEqual(Guid.Empty.ToString(), company.Id);
            }
        }

        [Fact]
        public void Should_update_stored_entity()
        {
            using (var documentStore = newDocumentStore())
            {
                var session = documentStore.OpenSession();
                var company = new Company { Name = "Company 1" };
                session.Store(company);
                var id = company.Id;
                company.Name = "Company 2";
                session.SaveChanges();
                var companyFound = session.Load<Company>(company.Id);
                Assert.Equal("Company 2", companyFound.Name);
                Assert.Equal(id, company.Id);
            }
        }

        [Fact]
        public void Should_update_retrieved_entity()
        {
            using (var documentStore = newDocumentStore())
            {
                var session1 = documentStore.OpenSession();
                var company = new Company { Name = "Company 1" };
                session1.Store(company);
                var companyId = company.Id;

                var session2 = documentStore.OpenSession();
                var companyFound = session2.Load<Company>(companyId);
                companyFound.Name = "New Name";
                session2.SaveChanges();

                Assert.Equal("New Name", session2.Load<Company>(companyId).Name);
            }
        }

        [Fact]
        public void Should_retrieve_all_entities()
        {
            using (var documentStore = newDocumentStore())
            {
                var session1 = documentStore.OpenSession();
                session1.Store(new Company { Name = "Company 1" });
                session1.Store(new Company { Name = "Company 2" });

                var session2 = documentStore.OpenSession();
                var companyFound = session2.GetAll<Company>();

                Assert.Equal(2, companyFound.Count);
            }
        }

        public void Dispose()
        {
            Thread.Sleep(100);
            Directory.Delete(path, true);
        }
    }

    public class Company
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string Address3 { get; set; }
        public List<Contact> Contacts { get; set; }
        public int Phone { get; set; }
    }

    public class Contact
    {
        public string FirstName { get; set; }
        public string Surname { get; set; }
        public string Email { get; set; }
    }
}