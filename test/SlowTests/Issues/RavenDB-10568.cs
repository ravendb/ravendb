using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10568 : RavenTestBase
    {
        private class Document
        {
            public string Id { get; set; }
        }

        [Fact]
        public void MetadataIsAvailableOnBeforeStoreEvent()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.OnBeforeStore += (sender, args) =>
                    {
                        args.DocumentMetadata["Value"] = "a";
                    };

                    session.Store(new Document
                    {
                        Id = "my-id/123"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var s = session.Load<Document>("my-id/123");
                    Assert.Equal("a", session.Advanced.GetMetadataFor(s)["Value"]);

                }
            }
        }

        [Fact]
        public void MetadataOnNewDocumentIsAvailableOnBeforeStoreTest()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    string url = null;
                    session.Advanced.OnBeforeStore += (sender, args) =>
                    {
                        // Access the document metadata so I can create a new entry in the Trie using the Patch API
                        url = (string)args.DocumentMetadata["Url"];
                    };

                    // Create a new document
                    var document = new Document
                    {
                        Id = "my-id/123"
                    };

                    session.Store(document);

                    // Add new metadata to the newly added document
                    var metadata = session.Advanced.GetMetadataFor(document);
                    metadata["Url"] = "/my-url";

                    session.SaveChanges();

                    Assert.Equal("/my-url", url);
                }
            }
        }

        [Fact]
        public void QueryIdStartsWithAndQueryOptimizerGeneratedIndexesDisabled_ShouldBeAbleToQuery()
        {
            Options options = new Options
            {
                ModifyDatabaseRecord = dr =>
                {
                    dr.Settings[RavenConfiguration.GetKey(x => x.Indexing.DisableQueryOptimizerGeneratedIndexes)] = "true";
                }
            };
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {

                    session.Store(new Document
                    {
                        Id = "my-id/123"
                    });

                    session.Store(new Document
                    {
                        Id = "my-id/333"
                    });
                    session.Store(new Document
                    {
                        Id = "my-id/1234"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Query<Document>()
                        .Where(x => x.Id.StartsWith("my-id/1"))
                        .ToList();

                    Assert.Equal(2, docs.Count);
                }
            }
        }
    }
}
