using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_22811 : RavenTestBase
{
    public RavenDB_22811(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax|RavenTestCategory.Indexes)]
    public void CoraxIndexMustNotThrowErrorsOnNonIndexedComplexFieldAfterSideBySideIndexing()
    {
        using (var store = GetDocumentStore(new Options
               {
                   RunInMemory = false
               }))
        {
            new ComplexFieldIndex(SearchEngineType.Lucene).Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Employee()
                {
                    FirstName = "Joe",
                    LastName = "Doe",
                    Address = new Address()
                    {
                        City = "NY",
                        Country = "USA"
                    }
                }, "employees/1");

                session.Store(new Employee()
                {
                    FirstName = "Foo",
                    LastName = "Bar",
                    Address = new Address()
                    {
                        City = "NY",
                        Country = "USA"
                    }
                }, "employees/2");

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            // switch to Corax (side-by-side indexing)
            var coraxIndex = new ComplexFieldIndex(SearchEngineType.Corax);
            coraxIndex.Execute(store);

            Indexes.WaitForIndexing(store);

            // ensure no index errors
            var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation([coraxIndex.IndexName]));

            Assert.Empty(indexErrors.SelectMany(x => x.Errors));

            // update document
            using (var session = store.OpenSession())
            {
                session.Store(new Employee()
                {
                    FirstName = "Foo",
                    LastName = "Bar",
                    Address = new Address()
                    {
                        City = "NY",
                        Country = "USA"
                    }
                }, "employees/1");

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            // ensure no index errors after the document update
            indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation([coraxIndex.IndexName]));

            Assert.Empty(indexErrors.SelectMany(x => x.Errors));
        }
    }

    private class ComplexFieldIndex : AbstractIndexCreationTask<Employee>
    {
        public ComplexFieldIndex(SearchEngineType engine)
        {
            Map = employees => from e in employees
                select new
                {
                    e.Address // complex field
                };

            SearchEngineType = engine;
        }
    }
}
