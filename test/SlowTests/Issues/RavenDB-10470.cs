using Tests.Infrastructure;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenLoadFromProjection : RavenTestBase
    {
        public RavenLoadFromProjection(ITestOutputHelper output) : base(output)
        {
        }

        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public IReadOnlyList<ChildReference> Children { get; set; }
        }

        private class ChildReference
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanProject(Options options)
        {
            Document doc;
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var child1 = new Document
                    {
                        Name = "Jerry"
                    };
                    var child2 = new Document
                    {
                        Name = "Bobby"
                    };

                    session.Store(child1);
                    session.Store(child2);

                    doc = new Document
                    {
                        Name = "parent",
                        Children = new List<ChildReference>
                        {
                            new ChildReference
                            {
                                Id = child1.Id
                            },
                            new ChildReference
                            {
                                Id = child2.Id
                            }
                        }
                    };

                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from result in session.Query<Document>()
                                .Customize(x => x.WaitForNonStaleResults())
                                .Where(x => x.Id == doc.Id)
                                let children = RavenQuery.Load<Document>(result.Children.Select(x => x.Id))
                                select new
                                {
                                    ChildNames = children.Select(x => x.Name).ToList()
                                };

                    var docs = query.ToList();

                    Assert.Equal(1, docs.Count);
                    Assert.Equal(2, docs[0].ChildNames.Count);
                    Assert.Equal("Jerry", docs[0].ChildNames[0]);
                    Assert.Equal("Bobby", docs[0].ChildNames[1]);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanProject2(Options options)
        {
            Document doc;
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    doc = new Document
                    {
                        Children = new List<ChildReference>
                        {
                            new ChildReference
                            {
                                Id = "id",
                                Name = "child"
                            }
                        }
                    };
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var docs = session.Query<Document>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Id == doc.Id)
                        .Select(x => new
                        {
                            Children = x.Children
                                .Select(c => new
                                {
                                    c.Id,
                                    c.Name
                                })
                                .ToList()
                        })
                        .ToList();

                    Assert.Equal(1, docs.Count);
                    Assert.Equal(1, docs[0].Children.Count);
                    Assert.NotNull(docs[0].Children[0].Name);
                    Assert.NotNull(docs[0].Children[0].Id);
                }
            }
        }

    }
}
