using System.Linq;
using FastTests.Server.Documents.Indexing;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_13869:RavenTestBase
    {
        public RavenDB_13869(ITestOutputHelper output) : base(output)
        {
        }

        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MissingFieldsDataShouldBeCleared(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    //person
                    session.Store(new User
                    {
                        Name = "John",
                        LastName = "Doh"
                    },"users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Load<Person>("users/1");
                    var sessionOperations = (session as InMemoryDocumentSessionOperations);                    
                    session.Advanced.Clear();

                    session.Store(person, "users/2");                    
                    session.SaveChanges();                    
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/2");

                    // person doesn't have a LastName field
                    Assert.Null(user.LastName);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MissingFieldsDataShouldBeEvicted(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {                    
                    session.Store(new User
                    {
                        Name = "John",
                        LastName = "Doh"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Load<Person>("users/1");
                    var sessionOperations = (session as InMemoryDocumentSessionOperations);                    
                    session.Advanced.Evict(person);

                    session.Store(person, "users/2");                    
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/2");

                    // person doesn't have a LastName field
                    Assert.Null(user.LastName);
                }
            }
        }

        public class UsersIndex : AbstractIndexCreationTask<User>
        {
            public UsersIndex()
            {
                Map = users => from user in users
                               select new User
                               {
                                   Id = user.Id
                               };
            }
        }
        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void MissingFieldDataShouldNotBeStoredDuringStreamingQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new UsersIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John",
                        LastName = "Doh"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User, UsersIndex>().Customize(x => x.WaitForNonStaleResults()).Count();

                    var streamEnumerator = session.Advanced.Stream<Person>(session.Query<Person, UsersIndex>());

                    streamEnumerator.MoveNext();

                    // that's a test code, never do that in real life scenarios!
                    session.Store(streamEnumerator.Current.Document, "users/2");
                    streamEnumerator.MoveNext();
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Load<User>("users/2");
                    Assert.Null(person.LastName);
                }
            }
        }
    }
}
