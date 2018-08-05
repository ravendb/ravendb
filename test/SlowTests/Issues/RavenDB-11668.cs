using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11668 : RavenTestBase
    {
        [Fact]
        public void CanProjectPropertyNamedGroup()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var document = new Document
                    {
                        Name = "my-name",
                        Group = "my-group"
                    };
                    session.Store(document);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Document>()
                        .Select(x => new
                        {
                            x.Name,
                            x.Group
                        })
                        .Customize(x => x.WaitForNonStaleResults());

                    Assert.Equal("from Documents as __alias0 " +
                                 "select __alias0.Name, __alias0.'Group'"
                        , query.ToString());

                    var triggers = query.ToList();

                    Assert.NotEmpty(triggers);
                    Assert.Equal("my-name", triggers[0].Name);
                    Assert.Equal("my-group", triggers[0].Group);
                }
            }
        }

        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Group { get; set; }
        }
    }
}
