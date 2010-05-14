using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Raven.Client.Document;
using System.Reflection;
using Xunit;

namespace Raven.Client.Tests.Document
{
    public class ClientKeyGeneratorTests : BaseTest, IDisposable
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
            path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
            path = Path.Combine(path, "TestDb").Substring(6);
            var documentStore = new DocumentStore
            {
                Configuration =
                    {
                        RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
                        DataDirectory = path
                    }
            };
            documentStore.Initialise();
            return documentStore;
        }

        [Fact]
        public void IdIsSetFromGeneratorOnStore()
        {
            using (var store = NewDocumentStore())
            {
                // Ensure we're using the hi lo generator
                var generator = new HiLoKeyGenerator(store.DatabaseCommands, 10);
                store.Conventions.DocumentKeyGenerator =
                    entity => generator.GenerateDocumentKey(store.Conventions, entity);

                using (var session = store.OpenSession())
                {
                    Company company = new Company();
                    session.Store(company);

                    Assert.Equal("companies/1", company.Id);
                }
            }
        }

        [Fact]
        public void DifferentTypesWillHaveDifferentIdGenerators()
        {
            using (var store = NewDocumentStore())
            {
                // Ensure we're using the hi lo generator
                var generator = new HiLoKeyGenerator(store.DatabaseCommands, 10);
                store.Conventions.DocumentKeyGenerator =
                    entity => generator.GenerateDocumentKey(store.Conventions, entity);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company);
                    var contact = new Contact();
                    session.Store(contact);

                    Assert.Equal("companies/1", company.Id);
                    Assert.Equal("contacts/1", contact.Id);
                }
            }
        }

        [Fact]
        public void IdIsKeptFromGeneratorOnSaveChanges()
        {
            using (var store = NewDocumentStore())
            {
                // Ensure we're using the hi lo generator
                var generator = new HiLoKeyGenerator(store.DatabaseCommands, 10);
                store.Conventions.DocumentKeyGenerator =
                    entity => generator.GenerateDocumentKey(store.Conventions, entity);

                using (var session = store.OpenSession())
                {
                    Company company = new Company();
                    session.Store(company);
                    session.SaveChanges();

                    Assert.Equal("companies/1", company.Id);
                }
            }
        }

        [Fact]
        public void NoIdIsSetAndSoIdIsNullAfterStore()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.DocumentKeyGenerator = fun=> null;

                using (var session = store.OpenSession())
                {
                    Company company = new Company();
                    session.Store(company);

                    Assert.Null(company.Id);
                }
            }
        }

        [Fact]
        public void NoIdIsSetAndSoIdIsSetAfterSaveChanges()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.DocumentKeyGenerator = fun => null;

                using (var session = store.OpenSession())
                {
                    Company company = new Company();
                    session.Store(company);
                    session.SaveChanges();

                    Assert.NotNull(company.Id);
                }
            }
        }
    }
}
