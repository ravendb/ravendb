namespace Raven.Management.Client.Silverlight.Tests
{
    using System;
    using System.Linq;
    using Client;
    using Database;
    using Document;
    using Microsoft.Silverlight.Testing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json.Linq;
    using Raven.Client.Document;

    [TestClass]
    public class AsyncServerClientTests : SilverlightTest
    {
        private readonly DocumentTestClass DocumentWithId = new DocumentTestClass(new JsonDocument
                                                                                      {
                                                                                          Key =
                                                                                              Guid.NewGuid().ToString()
                                                                                      });

        public AsyncServerClientTests()
        {
            DatabaseAddress = "http://localhost:8080";
            Store = new DocumentStore
                        {
                            Url = DatabaseAddress
                        };

            Store.Initialize();
        }

        private string DatabaseAddress { get; set; }

        private DocumentStore Store { get; set; }

        [TestMethod]
        [Asynchronous]
        public void DocumentTest()
        {
            using (IAsyncDocumentSession session = Store.OpenAsyncSession())
            {
                session.Store(DocumentWithId);
                session.SaveChanges((saveResult) =>
                                        {
                                            Assert.IsNotNull(saveResult);
                                            Assert.IsTrue(saveResult.Count == 1);
                                            Assert.IsTrue(saveResult.First().IsSuccess);

                                            session.Load<DocumentTestClass>(DocumentWithId.Id, (loadResult) => Assert.IsTrue(loadResult.IsSuccess));


                                            session.Delete(saveResult.First().Data);
                                            session.SaveChanges((deleteResult) =>
                                                                    {
                                                                        Assert.IsNotNull(deleteResult);
                                                                        Assert.IsTrue(deleteResult.Count == 1);
                                                                        Assert.IsTrue(deleteResult.First().IsSuccess);
                                                                    });

                                            EnqueueDelay(3000);
                                            EnqueueTestComplete();
                                        });
            }
        }

        [TestMethod]
        [Asynchronous]
        public void AttachementTest()
        {
            var data = new byte[1000];
            var random = new Random();
            random.NextBytes(data);

            using (var client = new AsyncServerClient(DatabaseAddress, new DocumentConvention(), null))
            {
                client.AttachmentPut("key", null, data, new JObject(), (putResult) =>
                                                                           {
                                                                               Assert.IsNotNull(putResult);
                                                                               Assert.IsTrue(putResult.Count == 1);
                                                                               Assert.IsTrue(putResult.First().IsSuccess);

                                                                               client.AttachmentGet("key",
                                                                                                    (loadResult) =>
                                                                                                    Assert.IsTrue(loadResult.IsSuccess));

                                                                               client.AttachmentDelete("key",
                                                                                                       (deleteResult) =>
                                                                                                       {
                                                                                                           Assert.IsNotNull(deleteResult);
                                                                                                           Assert.IsTrue(deleteResult.Count == 1);
                                                                                                           Assert.IsTrue(deleteResult.First().IsSuccess);
                                                                                                       });

                                                                               EnqueueDelay(3000);
                                                                               EnqueueTestComplete();
                                                                           });
            }
        }

        #region Nested type: DocumentTestClass

        public class DocumentTestClass
        {
            public DocumentTestClass(JsonDocument document)
            {
                Document = document;
                Id = document.Key;
            }

            public string Id { get; set; }

            public JsonDocument Document { get; set; }
        }

        #endregion
    }
}