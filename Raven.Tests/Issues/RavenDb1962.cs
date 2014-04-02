using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Database.Linq.PrivateExtensions;
using Xunit;
using Raven.Tests.Bugs;
using Raven.Client;
using Raven.Client.Document;

namespace Raven.Tests.Issues
{
    public class RavenDb1962 : RavenTest
    {
        private const int Cntr = 5;

        [Fact]
        public async Task CanDisplayLazyValues()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {

                    await StoreDataAsync(store, session);

                    var userFetchTasks = LazyLoadAsync(store, session);
                    int i = 1;
                    foreach (var lazy in userFetchTasks)
                    {
                        var user = await lazy.Value;
                        Assert.Equal(user.Name, "Test User #" + i);
                        i++;
                    }

                }

            }
        }


        public async Task CanDisplayLazyRequestTimes_RemoteAndData()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {

                   await StoreDataAsync(store, session);
    
                    var userFetchTasks = LazyLoadAsync(store, session);


                    var requestTimes = await session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.NotNull(requestTimes.TotalClientDuration);
                    Assert.NotNull(requestTimes.TotalServerDuration);
                    Assert.Equal(Cntr, requestTimes.DurationBreakdown.Count);
                }

                using (var ses = store.OpenSession())
                {
                    var queryRes = ses.Query<User>().Where(x => x.Name.StartsWith("T")).ToList();
                    int i = 1;
                    foreach (var res in queryRes)
                    {

                        Assert.Equal(res.Name, "Test User #" + i);
                        i++;
                    }
                }

            }
        }


        public async Task StoreDataAsync(DocumentStore store, IAsyncDocumentSession session)
        {
           for (int i = 1; i <= Cntr; i++)
           {
               await session.StoreAsync(new User { Name = "Test User #" + i }, "users/" + i);
           }
            await session.SaveChangesAsync();

        }

        public List<Lazy<Task<User>>> LazyLoadAsync(DocumentStore store, IAsyncDocumentSession session)
        {
            var listTasks = new List<Lazy<Task<User>>>();
            for (int i = 1; i <= Cntr; i++)
            {
                var userFetchTask = session.Advanced.Lazily.LoadAsync<User>("users/" + i);

                listTasks.Add(userFetchTask);
             }
            return listTasks;

       }
       
    }
}
/*
In ISyncAdvancedSessionOperation I can use Lazily to load multiple documents in one round trip.
i.e.
session.Advanced.Lazily.Load<Document>(documentId1);
session.Advanced.Lazily.Load<Document>(documentId2);
session.Advanced.Lazily.Load<Document>(documentId3);
session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

Is it possible to get Lazy loading support on IAsyncAdvancedSessionOperations?

*/