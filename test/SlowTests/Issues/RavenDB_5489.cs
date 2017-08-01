using System;
using FastTests;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5489 : RavenTestBase
    {
        [Fact]
        public async Task IfIndexEncountersCorruptionItShouldBeMarkedAsErrored()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                Assert.Equal(IndexState.Normal, store.Admin.Send(new GetStatisticsOperation()).Indexes[0].State);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex("Users/ByName");
                index._indexStorage.SimulateCorruption = true;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Bob"
                    });

                    session.SaveChanges();
                }

                var result = SpinWait.SpinUntil(() => store.Admin.Send(new GetStatisticsOperation()).Indexes[0].State == IndexState.Error, TimeSpan.FromSeconds(5));
                Assert.True(result);

                using (var commands = store.Commands())
                {
                    var e = Assert.Throws<RavenException>(() => commands.Query(new IndexQuery { Query = $"FROM INDEX '{new Users_ByName().IndexName}'" }));
                    Assert.Contains("Simulated corruption", e.InnerException.Message);
                }

                var errors = store.Admin.Send(new GetIndexErrorsOperation(new[] { new Users_ByName().IndexName }));
                Assert.Equal(1, errors[0].Errors.Length);
                Assert.Contains("Simulated corruption", errors[0].Errors[0].Error);
            }
        }

        private class User
        {
            public string Name { get; set; }
        }

        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from u in users
                               select new
                               {
                                   u.Name
                               };
            }
        }
    }
}