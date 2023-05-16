//-----------------------------------------------------------------------
// <copyright file="QueryingByNull.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Queries
{
    public class QueryingByNull : RavenTestBase
    {
        public QueryingByNull(ITestOutputHelper output) : base(output)
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
                    session.Store(new Person());
                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "People/ByName",
                    Maps =
                    {
                        "from doc in docs.People select new { doc.Name}"
                    }
                }));

                using (var session = store.OpenSession())
                {
                    var q = from person in session.Query<Person>("People/ByName")
                            .Customize(x => x.WaitForNonStaleResults())
                        where person.Name == null
                        select person;
                    Assert.Equal(1, q.Count());
                }
            }
        }

        private class Person
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
