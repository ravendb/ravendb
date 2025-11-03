using System;
using System.Collections.Generic;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.AI
{
    public class EmbeddingsWithMetadataTest(ITestOutputHelper output) : RavenTestBase(output)
    {
        private class TestDocument
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public List<string> Tags { get; set; }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public void CanCombineDocumentAndMetadataFields()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "articles/1";
                
                using (var session = store.OpenSession())
                {
                    var doc = new TestDocument
                    {
                        Id = docId,
                        Name = "Technical Article",
                        Description = "Deep dive into vector embeddings",
                        Tags = new List<string> { "technical", "tutorial" }
                    };
                    
                    session.Store(doc);
                    
                    var metadata = session.Advanced.GetMetadataFor(doc);
                    metadata["author"] = "Jane Doe";
                    metadata["publishDate"] = new DateTime(2024, 1, 15);
                    metadata["seoDescription"] = "Learn about vector embeddings in RavenDB";
                    metadata["targetAudience"] = "developers";
                    
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDocument>(docId);
                    Assert.NotNull(doc);
                    
                    var metadata = session.Advanced.GetMetadataFor(doc);
                    Assert.Equal("Jane Doe", metadata["author"]);
                    Assert.Equal("developers", metadata["targetAudience"]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Ai)]
        public void CanHandleSystemMetadataFields()
        {
            using (var store = GetDocumentStore())
            {
                var docId = "system/1";
                
                using (var session = store.OpenSession())
                {
                    var doc = new TestDocument
                    {
                        Id = docId,
                        Name = "System Test Document"
                    };
                    
                    session.Store(doc);
                    
                    // Add custom metadata alongside system metadata
                    var metadata = session.Advanced.GetMetadataFor(doc);
                    metadata["customField"] = "Custom Value";
                    
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDocument>(docId);
                    var metadata = session.Advanced.GetMetadataFor(doc);
                    
                    // Verify system metadata exists
                    Assert.True(metadata.ContainsKey("@collection"));
                    Assert.True(metadata.ContainsKey("@id"));
                    Assert.True(metadata.ContainsKey("@change-vector"));
                    
                    // Verify custom metadata
                    Assert.Equal("Custom Value", metadata["customField"]);
                }
            }
        }
    }
}
