using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11149 : RavenTestBase
    {
        private class Document
        {
#pragma warning disable 169,649
            public string Id;
#pragma warning restore 169,649
            public string Field1;
            public string Field2;
            public string Field3;
            public string Field4;
            public string Field5;
        }

        private class DocumentIndex : AbstractIndexCreationTask<Document>
        {
            public DocumentIndex()
            {
                Map = docs => from d in docs
                              select new
                              {
                                  d.Field1,
                                  d.Field2,
                                  d.Field3,
                                  d.Field4,
                                  d.Field5,
                              };
            }
        }

        [Fact]
        public void EmptyParameterListShouldNotMatchSimple()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    new DocumentIndex().Execute(store);

                    session.Store(new Document
                    {
                        Field1 = "Field1",
                        Field2 = "Field2",
                        Field3 = "Field3",
                        Field4 = "Field4",
                        Field5 = "Field5",
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    string query = @"
from 
    index 'DocumentIndex' 
where 
    Field5 in ($px) ";
                    var results = session.Advanced.RawQuery<Document>(query)
                        .AddParameter("px", new[] { "Field5" })
                        .ToList();

                    Assert.Equal(1, results.Count);

                    results = session.Advanced.RawQuery<Document>(query)
                        .AddParameter("px", new string[0])
                        .ToList();

                    Assert.Equal(0, results.Count);
                }
            }
        }

        [Fact]
        public void EmptyParameterListShouldNotMatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    new DocumentIndex().Execute(store);

                    session.Store(new Document
                    {
                        Field1 = "Field1",
                        Field2 = "Field2",
                        Field3 = "Field3",
                        Field4 = "Field4",
                        Field5 = "Field5",
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    string query = @"
from 
    index 'DocumentIndex' 
where 
    Field1 = 'Field1' 
    and Field2 = 'Field2' 
    and Field3 = 'Field3' 
    and Field4 in ('Field4') 
    and not Field1 in ('no-match') 
    and Field3 = 'Field3' 
    and Field5 in ($px) 
    and Field3 in ('Field3') 
    and Field4 in ('Field4')
order by 
    Field1, Field2, Field3 desc 
select 
    Field1, Field2, Field3, Field4, Field5";
                    var results = session.Advanced.RawQuery<Document>(query)
                        .AddParameter("px", new[] { "Field5" })
                        .ToList();
                    Assert.Equal(1, results.Count);

                    results = session.Advanced.RawQuery<Document>(query)
                        .AddParameter("px", new string[0])
                        .ToList();

                    Assert.Equal(0, results.Count);
                }
            }
        }
    }
}
