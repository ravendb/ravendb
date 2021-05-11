using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Blittable;
using FastTests.Utils;
using Raven.Client;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Linq
{
    public class CanQueryAndIncludeRevisions : RavenTestBase
    {
        public CanQueryAndIncludeRevisions(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task CanQueryAndIncludeRevisionsTest()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/omer";
                
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                        {
                            Name = "Omer",
                        },
                        id);
                    
                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.ContractRevision, changeVector);
                    await session.SaveChangesAsync();
                }
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Advanced
                                            .AsyncRawQuery<User>(@"from Users as u include revisions(  )")
                                            .ToListAsync();

                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);
                    
                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }   
            }
        }
        
        private class User
        {
            public string Name { get; set; }
            public string ContractRevision { get; set; }
        }
        
        private class Company
        {
            public string Name { get; set; }
        }
    }
}
