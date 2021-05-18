using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
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
                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>("from Users include revisions($p0)")
                        .AddParameter("p0", "ContractRevision")
                        .ToListAsync();
                    
                 

                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }   
                
                using (var session = store.OpenAsyncSession())
                {
                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(0, session.Advanced.NumberOfRequests);
                }   
            }
        }
        
         [Fact]
        public async Task CanQueryAndIncludeRevisionsArrayTest()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();
                
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
                    
                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);
                    
                    await session.SaveChangesAsync(); 
                    
                    cvList.Add(changeVector);
                    
                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    
                    changeVector =  metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);
                    
                    await session.SaveChangesAsync(); 
                    
                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);
                    
                    await session.SaveChangesAsync();
                    
                }
                
                // using (var session = store.OpenAsyncSession())
                // {
                //      var query = await session.Advanced
                //         .AsyncRawQuery<User>("from Users as u include revisions(u.FirstRevision, u.SecondRevision, u.ThirdRevision)")
                //         .ToListAsync();
                //
                //     var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                //     var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                //     var revision3 = await session.Advanced.Revisions.GetAsync<User>(cvList[2]);
                //
                //     Assert.NotNull(revision1);
                //     Assert.NotNull(revision2);
                //     Assert.NotNull(revision3);
                //     Assert.Equal(1, session.Advanced.NumberOfRequests);
                // }
                
                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>("from Users as u include revisions($p0, $p1, $p2)")
                        .AddParameter("p0","u.FirstRevision")
                        .AddParameter("p1","u.SecondRevision")
                        .AddParameter("p2","u.ThirdRevision")
                        .ToListAsync();
                    
                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revision3 = await session.Advanced.Revisions.GetAsync<User>(cvList[2]);
                
                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
        private class User
        {
            public string Name { get; set; }
            public string ContractRevision { get; set; }
            public string FirstRevision { get; set; }
            
            public string SecondRevision { get; set; }
            
            public string ThirdRevision { get; set; }
        }
    }
}
