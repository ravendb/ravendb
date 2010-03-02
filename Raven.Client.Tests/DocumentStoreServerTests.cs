using System;
using Raven.Server;
using Xunit;

namespace Raven.Client.Tests
{
    public class DocumentStoreServerTests : BaseTest
    {
        [Fact]
        public void Should_insert_into_db_and_set_id()
        {
            DivanServer.EnsureCanListenToWhenInNonAdminContext(8080);
            using (var server = new DivanServer(DbName, 8080))
            {
                var documentStore = new DocumentStore("localhost", 8080);
                documentStore.Initialise();

                var session = documentStore.OpenSession();
                var entity = new Company { Name = "Company" };
                session.Store(entity);

                Assert.NotEqual(Guid.Empty.ToString(), entity.Id);
            }
        }

        [Fact]
        public void Should_update_stored_entity()
        {
            DivanServer.EnsureCanListenToWhenInNonAdminContext(8080);
            using (var server = new DivanServer(DbName, 8080))
            {
                var documentStore = new DocumentStore("localhost", 8080);
                documentStore.Initialise();

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
            DivanServer.EnsureCanListenToWhenInNonAdminContext(8080);
            using (var server = new DivanServer(DbName, 8080))
            {
                var documentStore = new DocumentStore("localhost", 8080);
                documentStore.Initialise();

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
    }
}