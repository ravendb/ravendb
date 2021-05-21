using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16724 : RavenTestBase
    {
        public RavenDB_16724(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task CanIncrementNumberOfRequests()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/Hibernating";
                
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                        {
                            Name = "Hibernating",
                        },
                        id);

                    byte[] byteArray = Encoding.UTF8.GetBytes("Rhinos");
                    
                    await session.SaveChangesAsync();
                     
                    session.Advanced.Attachments.Store("users/Hibernating",
                        "invoice.pdf",
                        new MemoryStream(byteArray),
                        "application/pdf");
                    
                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(2, metadatas.Count);
                    
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                { 
                    session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

                    var attachment = await session.Advanced.Attachments.GetAsync("users/Hibernating",
                        "invoice.pdf");

                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(id,DateTime.Now);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(changeVector:changeVector);
                    var revision3 = await session.Advanced.Revisions.GetAsync<User>(new[]{changeVector});
                    
                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    Assert.NotNull(attachment);

                    Assert.Equal(4, session.Advanced.NumberOfRequests);
                }   
                
                using (var session = store.OpenSession())
                {
                    var revision1 =  session.Advanced.Revisions.Get<User>(id,DateTime.Now);
                    var revision2 =  session.Advanced.Revisions.Get<User>(changeVector:changeVector);
                    var revision3 =  session.Advanced.Revisions.Get<User>(new[]{changeVector});
                    
                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    
                    session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
                    
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                }   
            }
        }
        private class User
        {
            public string Name { get; set; }
        }
    }    
}
