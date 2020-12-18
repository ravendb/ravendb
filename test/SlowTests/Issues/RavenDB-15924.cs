using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15924 : RavenTestBase
    {
        public RavenDB_15924(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public void ShouldWork()
        {
            using var store = GetDocumentStore();
            new DocsIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1", StrVal = "\"" }); // this is: "
                session.Store(new Doc { Id = "doc-2", StrVal = "\"\"" }); // this is: ""
                session.Store(new Doc { Id = "doc-3", StrVal = "\"\\\"\\\"\"" }); // this is: "\"\""
                session.Store(new Doc { Id = "doc-4", StrVal = "\"\\\"\\\"\\\"\"" }); // this is: "\"\"\""
                session.SaveChanges();
            }

            WaitForIndexing(store);

            string Escape(string s)
            {
                return s.Replace("\\", "\\\\").Replace("\"", "\\\"");
            }
            
            using (var session = store.OpenSession())
            {
                // Exact search for one quote
                //from index 'DocsIndex' where search(StrVal, '\"')
                var q1 = session.Query<Doc, DocsIndex>().Search(x => x.StrVal, Escape("\""));
                Assert.Equal(1, q1.Count());

                // Search for one quote prefix
                // from index 'DocsIndex' where search(StrVal, '\"*')
                var q2 = session.Query<Doc, DocsIndex>().Search(x => x.StrVal, Escape("\"*"));
                Assert.Equal(4, q2.Count());

                // Exact search for two quotes
                var q3 = session.Query<Doc, DocsIndex>().Search(x => x.StrVal,Escape( "\"\""));
                Assert.Equal(1, q3.Count());

                // Exact search for two escaped quotes enclosed in quotes
                // from index 'DocsIndex' where search(StrVal, '\"\\\"\\\"\"')
                var q4 = session.Query<Doc, DocsIndex>().Search(x => x.StrVal, Escape("\"\\\"\\\"\""));
                Assert.Equal(1, q4.Count());
                
                // Exact search for three escaped quotes enclosed in quotes
                // from index 'DocsIndex' where search(StrVal, '\"\\\"\\\"\\\"\"')
                var q5 = session.Query<Doc, DocsIndex>().Search(x => x.StrVal,Escape( "\"\\\"\\\"\\\"\""));
                Assert.Equal(1, q5.Count());
            }
        }

        private class DocsIndex : AbstractIndexCreationTask<Doc>
        {
            public DocsIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        doc.Id,
                        doc.StrVal,
                    };

                Indexes.Add(x => x.StrVal, FieldIndexing.Exact);

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class Doc
        {
            public string Id { get; set; }
            public string StrVal { get; set; }
        }
    }
}
