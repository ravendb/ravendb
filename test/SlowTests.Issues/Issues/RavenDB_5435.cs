using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5435 : RavenTestBase
    {
        public RavenDB_5435(ITestOutputHelper output) : base(output)
        {
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

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanCompact()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                await new Users_ByName().ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 1000; i++)
                    {
                        session.Store(new User
                        {
                            Name = i.ToString()
                        });
                    }

                    session.SaveChanges();
                }

                await Indexes.WaitForIndexingAsync(store);

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    Indexes = new[] { new Users_ByName().IndexName }
                }));

                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<User, Users_ByName>().Count();
                    Assert.Equal(1000, count);
                }
            }
        }
    }
}
