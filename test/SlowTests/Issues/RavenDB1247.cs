using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB1247 : RavenTestBase
    {
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

        [Fact]
        public void DocumentWithoutIdPropertyIsStored_HashSymbolInId_HashSymbolNotRemovedFromId()
        {
            const string TEST_DOCUMENT_ID = "FooDocument/#123";

            using (var store = GetDocumentStore())
            {
                var newDocument = new FooDocumentWithoutIdProperty()
                {
                    Property1 = "ABC",
                    Property2 = 123456
                };

                using (var commands = store.Commands())
                {
                    commands.Put(TEST_DOCUMENT_ID, null, newDocument,
                        new Dictionary<string, object>
                        {
                            {Constants.Documents.Metadata.Collection, "FooDocumentWithoutIdProperties"}
                        });
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {

                    var relevantDocuments = session.Query<FooDocumentWithoutIdProperty>();
                    var fetchedDocument = relevantDocuments.FirstOrDefault();
                    Assert.NotNull(fetchedDocument);

                    var fetchedDocumentId = session.Advanced.GetDocumentId(fetchedDocument);
                    Assert.Equal(TEST_DOCUMENT_ID, fetchedDocumentId);
                }

            }
        }

        [Fact]
        public void DocumentWithIdPropertyIsStored_HashSymbolInId_HashSymbolNotRemovedFromId()
        {
            const string TEST_DOCUMENT_ID = "FooDocument/#123";

            using (var store = GetDocumentStore())
            {
                var newDocument = new FooDocumentWithIdProperty()
                {
                    Id = TEST_DOCUMENT_ID,
                    Property1 = "ABC",
                    Property2 = 123456
                };

                using (var commands = store.Commands())
                {
                    commands.Put(TEST_DOCUMENT_ID, null, newDocument,
                        new Dictionary<string, object>
                        {
                            {Constants.Documents.Metadata.Collection, "FooDocumentWithIdProperty"}
                        });
                }

                using (var session = store.OpenSession())
                {
                    var fetchedDocument = session.Load<FooDocumentWithIdProperty>(TEST_DOCUMENT_ID);
                    Assert.NotNull(fetchedDocument);
                }
            }
        }
    }
}
