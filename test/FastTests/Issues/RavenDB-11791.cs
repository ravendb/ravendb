using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_11791 : RavenTestBase
    {

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
        [Fact]
        public void Test()
        {
            using (var store = GetDocumentStore())
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
