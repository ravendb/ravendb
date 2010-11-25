namespace Raven.Management.Client.Silverlight.Tests
{
    using System;
    using System.Linq;
    using Microsoft.Silverlight.Testing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json.Linq;
    using Raven.Client.Document;
    using Raven.Database;
    using Raven.Database.Data;
    using Raven.Management.Client.Silverlight.Client;
    using Raven.Management.Client.Silverlight.Collections;
    using Raven.Management.Client.Silverlight.Document;
    using Raven.Management.Client.Silverlight.Indexes;
    using Raven.Management.Client.Silverlight.Statistics;

    [TestClass]
    public class AsyncServerClientTests : SilverlightTest
    {
        private readonly DocumentTestClass DocumentWithId = new DocumentTestClass(new JsonDocument
                                                                                      {
                                                                                          Key = Guid.NewGuid().ToString()
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

        //[TestMethod]
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

        //[TestMethod]
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

        //[TestMethod]
        [Asynchronous]
        public void AttachmentGetAllTest()
        {
            var data = new byte[1000];
            var random = new Random();
            random.NextBytes(data);

            bool contiunueWithTest = false;

            using (var client = new AsyncServerClient(DatabaseAddress, new DocumentConvention(), null))
            {
                client.AttachmentPut("key1", null, data, new JObject(), (putResult1) =>
                                                                            {
                                                                                Assert.IsNotNull(putResult1);
                                                                                Assert.IsTrue(putResult1.Count == 1);
                                                                                Assert.IsTrue(putResult1.First().IsSuccess);

                                                                                client.AttachmentPut("key2", null, data, new JObject(), (putResult2) =>
                                                                                                                                            {
                                                                                                                                                Assert.IsNotNull(putResult2);
                                                                                                                                                Assert.IsTrue(putResult2.Count == 1);
                                                                                                                                                Assert.IsTrue(putResult2.First().IsSuccess);

                                                                                                                                                EnqueueConditional(() => true);
                                                                                                                                            });
                                                                            });
            }
        }

        [TestMethod, Asynchronous]
        public void StatisticsGetTest()
        {
            using (var statisticsSession = new AsyncStatisticsSession(DatabaseAddress))
            {
                statisticsSession.Load((result) =>
                                           {
                                               Assert.IsNotNull(result);
                                               Assert.IsTrue(result.IsSuccess);
                                               Assert.IsNotNull(result.Data.Errors);
                                               Assert.IsNotNull(result.Data.Indexes);
                                               Assert.IsNotNull(result.Data.StaleIndexes);
                                               Assert.IsNotNull(result.Data.Triggers);

                                               EnqueueTestComplete();
                                           });
            }
        }

        [TestMethod, Asynchronous]
        public void QueryTest()
        {
            using (var indexSession = new AsyncIndexSession(DatabaseAddress))
            {
                indexSession.Query("Raven/DocumentsByEntityName", new IndexQuery(), null, (result) =>
                                                                                              {
                                                                                                  Assert.IsTrue(result.IsSuccess);

                                                                                                  EnqueueTestComplete();
                                                                                              });
            }
        }

        [TestMethod, Asynchronous]
        public void CollectionLoadTest()
        {
            using (var collectionSession = new AsyncCollectionSession(DatabaseAddress))
            {
                collectionSession.Load("Posts", (result) =>
                                                    {
                                                        Assert.IsTrue(result.IsSuccess);

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