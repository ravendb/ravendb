using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11123 : RavenTestBase
    {
        public RavenDB_11123(ITestOutputHelper output) : base(output)
        {
        }

        private class Index1 : AbstractIndexCreationTask<Company>
        {
            public Index1()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       Name = c.Name,
                                       NameExact = c.Name,
                                       ExternalId = c.ExternalId
                                   };

                Index("NameExact", FieldIndexing.Exact);
            }
        }

        [Fact]
        public void CanUseNullInWhereLuceneOrPassExact()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "C1",
                        ExternalId = null
                    });

                    session.Store(new Company
                    {
                        Name = "C2",
                        ExternalId = "E2"
                    });

                    
                    session.Store(new Company
                    {
                        Name = "C3",
                        ExternalId = null
                    });

                    session.SaveChanges();
                }

                new Index1().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Advanced
                        .DocumentQuery<Company, Index1>()
                        .WhereLucene("ExternalId", null)
                        .ToList();

                    Assert.Equal(2, results.Count);

                    results = session
                        .Advanced
                        .DocumentQuery<Company, Index1>()
                        .WhereLucene("ExternalId", null)
                        .AndAlso()
                        .WhereLucene("Name", "C3")
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("C3", results[0].Name);
                }

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereLucene("ExternalId", null)
                        .ToList();

                    Assert.Equal(2, results.Count);

                    results = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereLucene("ExternalId", null)
                        .AndAlso()
                        .WhereLucene("Name", "C3")
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("C3", results[0].Name);
                }
                
                using (var session = store.OpenSession())
                {
                    var results = session
                        .Advanced
                        .DocumentQuery<Company, Index1>()
                        .WhereLucene("ExternalId", "NULL_VALUE", exact: true)
                        .ToList();

                    Assert.Equal(2, results.Count);

                    results = session
                        .Advanced
                        .DocumentQuery<Company, Index1>()
                        .WhereLucene("ExternalId", "NULL_VALUE", exact: true)
                        .AndAlso()
                        .WhereLucene("Name", "C3")
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("C3", results[0].Name);
                }

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereLucene("ExternalId", "NULL_VALUE", exact: true)
                        .ToList();

                    Assert.Equal(2, results.Count);

                    results = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereLucene("ExternalId", "NULL_VALUE", exact: true)
                        .AndAlso()
                        .WhereLucene("Name", "C3")
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("C3", results[0].Name);
                }

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Advanced
                        .DocumentQuery<Company, Index1>()
                        .WhereLucene("ExternalId", "NULL_VALUE AND NameExact:C3", exact: true)
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("C3", results[0].Name);
                }
            }
        }
    }
}
