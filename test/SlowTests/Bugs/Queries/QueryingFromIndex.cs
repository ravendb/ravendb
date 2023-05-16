//-----------------------------------------------------------------------
// <copyright file="QueryingFromIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Queries
{
    public class QueryingFromIndex : RavenTestBase
    {
        public QueryingFromIndex(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void LuceneQueryWithIndexIsCaseInsensitive(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var definition = new IndexDefinitionBuilder<Company>("CompanyByName")
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Name
                                  }
                }.ToIndexDefinition(store.Conventions);

                store.Maintenance.Send(new PutIndexesOperation(definition));

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Google" });
                    session.Store(new Company
                    {
                        Name =
                            "HibernatingRhinos"
                    });
                    session.SaveChanges();

                    var company =
                        session.Advanced.DocumentQuery<Company>("CompanyByName")
                            .WhereEquals("Name", "Google")
                            .WaitForNonStaleResults()
                            .FirstOrDefault();

                    Assert.NotNull(company);
                }
            }
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void LinqQueryWithIndexIsCaseInsensitive(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var definition = new IndexDefinitionBuilder<Company>("CompanyByName")
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Name
                                  }
                }.ToIndexDefinition(store.Conventions);
                store.Maintenance.Send(new PutIndexesOperation(definition));


                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Google" });
                    session.Store(new Company
                    {
                        Name =
                            "HibernatingRhinos"
                    });
                    session.SaveChanges();

                    var company =
                        session.Query<Company>("CompanyByName")
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Name == "Google")
                            .FirstOrDefault();

                    Assert.NotNull(company);
                }
            }
        }

        private class Company
        {
            public decimal AccountsReceivable { get; set; }
            public string Id { get; set; }
            public string Name { get; set; }
            public string Address1 { get; set; }
            public string Address2 { get; set; }
            public string Address3 { get; set; }
            public int Phone { get; set; }
        }
    }
}
