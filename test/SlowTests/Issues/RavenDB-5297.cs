using System.Linq;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_5297 : RavenTestBase
    {
        public RavenDB_5297(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public override string IndexName => "Users/ByName";

            public UsersByName()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        [Fact]
        public void QueryLuceneMinusOperator()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                new UsersByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "First"
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Second"
                    }, "users/2");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var requestExecuter = store.GetRequestExecutor();

                using (var session = store.OpenSession())
                using (requestExecuter.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new QueryCommand((InMemoryDocumentSessionOperations)session, new IndexQuery { Query = "FROM INDEX 'Users/ByName' WHERE exists(Name) AND Name = 'First' ORDER BY Name DESC" });

                    requestExecuter.Execute(command, context);

                    var query = command.Result;
                    Assert.Equal(query.TotalResults, 1);
                }
            }
        }

        [Fact]
        public void QueryLuceneNotOperator()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                new UsersByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "First"
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Second"
                    }, "users/2");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var requestExecuter = store.GetRequestExecutor();

                using (var session = store.OpenSession())
                using (requestExecuter.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new QueryCommand((InMemoryDocumentSessionOperations)session, new IndexQuery { Query = "FROM INDEX 'Users/ByName' WHERE exists(Name) AND NOT Name = 'Second'" });

                    requestExecuter.Execute(command, context);

                    var query = command.Result;
                    Assert.Equal(query.TotalResults, 1);
                }
            }
        }
    }
}
