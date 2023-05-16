//-----------------------------------------------------------------------
// <copyright file="QueryingByNegative.cs" company="Hibernating Rhinos LTD">
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
    public class QueryingByNegative : RavenTestBase
    {
        public QueryingByNegative(ITestOutputHelper output) : base(output)
        {
        }


        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanQueryByNullUsingLinq(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Age = -5
                    });
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "People/ByAge",
                    Maps =
                    {
                        "from doc in docs.People select new { doc.Age}"
                    },
                    Fields =
                    {
                        {
                            "Age", new IndexFieldOptions
                            {
                                Indexing = FieldIndexing.Exact
                            }
                        }
                    }
                }));

                using (var session = store.OpenSession())
                {
                    var q = from person in session.Query<Person>("People/ByAge")
                            .Customize(x => x.WaitForNonStaleResults())
                        where person.Age == -5
                        select person;
                    Assert.Equal(1, q.Count());
                }
            }
        }

        private class Person
        {
            public string Id { get; set; }
            public int Age { get; set; }
        }
    }
}
