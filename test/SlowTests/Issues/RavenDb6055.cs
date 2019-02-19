using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Sparrow.Server;
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

        [Fact]
        public async Task CreatingNewAutoIndexWillDeleteSmallerOnes()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.TimeBeforeDeletionOfSupersededAutoIndex)] = "0"
            }))
            {
                IndexDefinition[] indexes;
                using (var session = store.OpenAsyncSession())
                {
                    await session.Query<User>()
                        .Where(x => x.FirstName == "Alex")
                        .ToListAsync();

                    indexes = await store.Maintenance.SendAsync(new GetIndexesOperation(0, 25));
                    Assert.Equal(1, indexes.Length);
                    Assert.Equal("Auto/Users/ByFirstName", indexes[0].Name);
                }

                var mre = new AsyncManualResetEvent();

                var allIndexes = store.Changes()
                    .ForAllIndexes();
                allIndexes
                    .Subscribe(x =>
                    {
                        if (x.Type == IndexChangeTypes.IndexRemoved)
                            mre.Set();
                    });
                await allIndexes.EnsureSubscribedNow();
                using (var session = store.OpenAsyncSession())
                {
                    await session.Query<User>()
                        .Where(x => x.LastName == "Smith")
                        .ToListAsync();
                }

                Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(30)));

                WaitForUserToContinueTheTest(store);
                
                indexes = await store.Maintenance.SendAsync(new GetIndexesOperation(0, 25));
                Assert.Equal(1, indexes.Length);
                Assert.Equal("Auto/Users/ByFirstNameAndLastName", indexes[0].Name);
            }
        }
    }
}
