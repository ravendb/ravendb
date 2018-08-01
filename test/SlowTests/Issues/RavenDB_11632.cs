using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11632 : RavenTestBase
    {
        [Fact]
        public void ShouldBeAbleToStoreExistingDocumentWithEarlierIdAfterOldDocumentWasDeleted()
        {
            const string id = "docs/1";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Document
                    {
                        Id = id,
                        Text = "text-1"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Document>(id);
                    Assert.NotNull(doc);

                    session.Delete(doc);

                    doc = new Document
                    {
                        Id = id,
                        Text = "text-2"
                    };
                    
                    session.Store(doc);

                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Document>(id);
                    Assert.NotNull(doc);
                    Assert.Equal("text-2", doc.Text);
                }
            }
        }
        
        [Fact]
        public void ShouldBeAbleToStoreNewDocumentWithEarlierIdAfterOldDocumentWasDeleted()
        {
            const string id = "docs/1";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Document
                    {
                        Id = id,
                        Text = "text-1"
                    });
                    
                    var doc = session.Load<Document>(id);
                    Assert.NotNull(doc);
                    Assert.Equal("text-1", doc.Text);

                    session.Delete(doc);

                    doc = new Document
                    {
                        Id = id,
                        Text = "text-2"
                    };
                    
                    session.Store(doc);

                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Document>(id);
                    Assert.NotNull(doc);
                    Assert.Equal("text-2", doc.Text);
                }
            }
        }


        private class Document
        {
            public string Id { get; set; }
            public string Text { get; set; }
        }
    }
}
