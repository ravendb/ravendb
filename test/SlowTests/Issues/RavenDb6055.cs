using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Notifications;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDb6055 : RavenTestBase
    {
        private class User
        {
#pragma warning disable 169,649
            public string FirstName;
            public string LastName;
#pragma warning restore 169,649
        }

        [Fact(Skip = "RavenDB-6571")]
        public async Task CreatingNewAutoIndexWillDeleteSmallerOnes()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.Query<User>()
                        .Where(x => x.FirstName == "Alex")
                        .ToListAsync();

                    var indexes = await store.Admin.SendAsync(new GetIndexesOperation(0, 25));
                    Assert.Equal(1, indexes.Length);
                    Assert.Equal("Auto/Users/ByFirstName", indexes[0].Name);
                }

                var mre = new ManualResetEventSlim();

                using (store.Changes().ForAllIndexes()
                    .Subscribe(x =>
                    {
                        if (x.Type == IndexChangeTypes.IndexRemoved)
                            mre.Set();
                    }))
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.Query<User>()
                            .Where(x => x.LastName == "Smith")
                            .ToListAsync();
                    }

                    Assert.True(mre.Wait(TimeSpan.FromSeconds(15)));

                    var indexes = await store.Admin.SendAsync(new GetIndexesOperation(0, 25));
                    Assert.Equal("Auto/Users/ByFirstNameAndLastName", indexes[0].Name);
                }
            }
        }
    }
}