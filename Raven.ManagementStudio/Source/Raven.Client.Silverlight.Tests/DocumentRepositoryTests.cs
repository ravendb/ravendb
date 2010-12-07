using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using Raven.Client.Silverlight.Data;
using System;
using Raven.Client.Silverlight.Document;

namespace Raven.Client.Silverlight.Tests
{
    [TestClass]
    public class DocumentRepositoryTests
    {
        public IAsyncDocumentSession Session { get; set; }

        [TestMethod]
        public void GetDocumentTest()
        {
        }

        [TestMethod]
        public void GetAllDocumentsTest()
        {
        }

        [TestMethod]
        public void SaveDocumentWithIDTest()
        {
        }

        [TestMethod]
        public void SaveDocumentWithoutIDTest()
        {
        }

        [TestMethod]
        public void DocumentBatchingTest()
        {
            var store = new DocumentStore("http://localhost:8080");
            Session = store.OpenAsyncSession();

            Session.StoreEntity(new JsonDocument()
                                    {
                                        Key = Guid.NewGuid().ToString(),
                                        DataAsJson = new JObject(),
                                        Metadata = new JObject()
                                    });

            Session.StoreEntity(new JsonDocument()
                                    {
                                        Key = Guid.NewGuid().ToString(),
                                        DataAsJson = new JObject(),
                                        Metadata = new JObject()
                                    });

            Session.StoreEntity(new JsonDocument()
                                    {
                                        Key = Guid.NewGuid().ToString(),
                                        DataAsJson = new JObject(),
                                        Metadata = new JObject()
                                    });

            Session.SaveChanges((result) => Assert.IsTrue(result.IsSuccess));
        }
    }
}