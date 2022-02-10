using System.Linq;
using FastTests.Server.Documents.Indexing;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_11791 : RavenTestBase
    {
        public RavenDB_11791(ITestOutputHelper output) : base(output)
        {
        }


        public class PeopleIndex : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex()
            {
                Map = people => from person in people
                                select new
                                {
                                    person.Name
                                };
            }
        }
        
        [Theory]
        [SearchEngineClassData]
        public void Test(string searchEngine)
        {
            using (var store = GetDocumentStore(Options.ForSearchEngine(searchEngine)))
            {
                new PeopleIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), "people/1");
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    session.Delete("people/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Person());
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                }

            }

        }
    }
}
