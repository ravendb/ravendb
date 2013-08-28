using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Rhino.Mocks;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB1247 : RavenTestBase
    {
        #region Helper Classes
        private class FooDocumentWithIdProperty
        {
            public string Id { get; set; }

            public string Property1 { get; set; }

            public int Property2 { get; set; }
        }

        private class FooDocumentWithoutIdProperty
        {
            public string Property1 { get; set; }

            public int Property2 { get; set; }
        }

        #endregion

        [Fact]
        public void DocumentWithoutIdPropertyIsStored_HashSymbolInId_HashSymbolNotRemovedFromId()
        {
            const string TEST_DOCUMENT_ID = "FooDocument/#123";

            using (var store = NewRemoteDocumentStore())
            {
                var newDocument = new FooDocumentWithoutIdProperty()
                {
                    Property1 = "ABC",
                    Property2 = 123456
                };

                store.DatabaseCommands.Put(TEST_DOCUMENT_ID, null, RavenJObject.FromObject(newDocument),
                    new RavenJObject
                    {
                        {Constants.RavenEntityName, "FooDocumentWithoutIdProperties"}
                    });

                while (store.DatabaseCommands.GetStatistics().StaleIndexes.Any())
                    Thread.Sleep(10);               

                using (var session = store.OpenSession())
                {
                    
                    var relevantDocuments = session.Query<FooDocumentWithoutIdProperty>();
                    var fetchedDocument = relevantDocuments.FirstOrDefault();
                    Assert.NotNull(fetchedDocument);

                    var fetchedDocumentId = session.Advanced.GetDocumentId(fetchedDocument);                    
                    Assert.Equal(TEST_DOCUMENT_ID,fetchedDocumentId);
                }

            }
        }

        [Fact]
        public void DocumentWithIdPropertyIsStored_HashSymbolInId_HashSymbolNotRemovedFromId()
        {
            const string TEST_DOCUMENT_ID = "FooDocument/#123";

            using (var store = NewRemoteDocumentStore())
            {
                var newDocument = new FooDocumentWithIdProperty()
                {
                    Id = TEST_DOCUMENT_ID,
                    Property1 = "ABC",
                    Property2 = 123456
                };

                store.DatabaseCommands.Put(TEST_DOCUMENT_ID, null, RavenJObject.FromObject(newDocument),
                                   new RavenJObject
                    {
                        {Constants.RavenEntityName, "FooDocumentWithIdProperty"}
                    });

                using (var session = store.OpenSession())
                {                    
                    var fetchedDocument = session.Load<FooDocumentWithIdProperty>(TEST_DOCUMENT_ID);
                    Assert.NotNull(fetchedDocument);
                }

            }
        }
    }
}
