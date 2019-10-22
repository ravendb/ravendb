using System;
using FastTests;
using Xunit;
using System.Linq;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class CaseSensitiveDeletes : RavenTestBase
    {
        public CaseSensitiveDeletes(ITestOutputHelper output) : base(output)
        {
        }

        private class Document
        {
            public string Id { get; set; }
        }

        [Fact]
        public void ShouldWork_WithCount()
        {
            using (var documentStore = GetDocumentStore())
            {
                for (int i = 0; i < 10; i++)
                {
                    using (var session = documentStore.OpenSession())
                    {
                        for (int j = 0; j < 60; j++)
                        {
                            var doc = new Document
                            {
                                Id = "CaseSensitiveIndex" + Guid.NewGuid()
                            };

                            session.Store(doc);
                        }
                        session.SaveChanges();
                        var deletes = session.Query<Document>().Customize(x => x.WaitForNonStaleResults()).ToList();
                        deletes.ForEach(session.Delete);
                        session.SaveChanges();


                        var count = session.Query<Document>().Customize(x => x.WaitForNonStaleResults()).Count();
                        Assert.Equal(0, count);
                    }
                }
            }
        }

        [Fact]
        public void ShouldWork_WithAnotherQuery()
        {
            using (var documentStore = GetDocumentStore())
            {
                for (int i = 0; i < 10; i++)
                {
                    using (var session = documentStore.OpenSession())
                    {
                        for (int j = 0; j < 60; j++)
                        {
                            var doc = new Document
                            {
                                Id = "CaseSensitiveIndex" + Guid.NewGuid()
                            };

                            session.Store(doc);
                        }

                        session.SaveChanges();
                        var deletes = session.Query<Document>().Customize(x => x.WaitForNonStaleResults()).ToList();
                        deletes.ForEach(session.Delete);
                        session.SaveChanges();

                        deletes = session.Query<Document>().Customize(x => x.WaitForNonStaleResults()).ToList();
                        Assert.Equal(0, deletes.Count);
                    }
                }
            }
        }

    }
}
