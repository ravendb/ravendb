using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Raven.Client.Document;
using System.Reflection;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Client.Tests.Document
{
    public class ClientKeyGeneratorTests : RemoteClientTest, IDisposable
    {
        private string path;

        #region IDisposable Members

        public void Dispose()
        {
            IOExtensions.DeleteDirectory(path);
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
            documentStore.Initialize();
            return documentStore;
        }

        [Fact]
        public void IdIsSetFromGeneratorOnStore()
        {
            using (var store = NewDocumentStore())
            {
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
        public void WhenDocumentAlreadyExists_Can_Still_Generate_Values()
        {
            using (var store = NewDocumentStore())
            {
                var mk = new MultiTypeHiLoKeyGenerator(store, 5);
                store.Conventions.DocumentKeyGenerator = o => mk.GenerateDocumentKey(store.Conventions, o);

                
                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company);
                    var contact = new Contact();
                    session.Store(contact);

                    Assert.Equal("companies/1", company.Id);
                    Assert.Equal("contacts/1", contact.Id);
                }

                mk = new MultiTypeHiLoKeyGenerator(store, 5);
                store.Conventions.DocumentKeyGenerator = o => mk.GenerateDocumentKey(store.Conventions, o);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company);
                    var contact = new Contact();
                    session.Store(contact);

                    Assert.Equal("companies/6", company.Id);
                    Assert.Equal("contacts/6", contact.Id);
                }
            }
        }

        [Fact]
        public void DoesNotLoseValuesWhenHighIsOver()
        {
            using (var store = NewDocumentStore())
            {
                var mk = new MultiTypeHiLoKeyGenerator(store, 5);
                for (int i = 0; i < 15; i++)
                {
                    Assert.Equal("companies/"+(i+1),
                        mk.GenerateDocumentKey(store.Conventions, new Company()));
                }
            }
        }


        [Fact]
        public void IdIsKeptFromGeneratorOnSaveChanges()
        {
            using (var store = NewDocumentStore())
            {
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
