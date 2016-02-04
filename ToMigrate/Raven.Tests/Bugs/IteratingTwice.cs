using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Rhino.Mocks.Constraints;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class IteratingTwice : RavenTest
    {
        [Fact]
        public void WillResultInTheSameResults()
        {
            using(GetNewServer())
            using(var store = new DocumentStore
            {
                Url = "http://localhost:8079"
            }.Initialize())
            {
                using(var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var query = s.Query<User>();

                    for (int i = 0; i < 5; i++)
                    {
                        foreach (var user in query)
                        {
                            Assert.NotNull(user.Id);
                        }
                    }
                }
            }
        }

        [Fact]
        public void WillResultInTheSameResults_Lucene()
        {
            using (GetNewServer())
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8079"
            }.Initialize())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var query = s.Advanced.DocumentQuery<User>();

                    for (int i = 0; i < 5; i++)
                    {
                        foreach (var user in query)
                        {
                            Assert.NotNull(user.Id);
                        }
                    }
                }
            }
        }

        [Fact]
        public void CanCancelToListAsync()
        {
            var users = Builder<User>.CreateListOfSize(5000).Build();			

            using (var store = NewRemoteDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    foreach(var user in users)
                        session.Store(user);

                    session.SaveChanges();
                }

                using(var cts = new CancellationTokenSource())
                using (var asyncSession = store.OpenAsyncSession())
                {
                    var query = asyncSession.Query<User>()
                                            .Where(x => x.Age > 10);

                    cts.Cancel(); //cancellation token is already marked as canceled.
                    var queryEnumerationTask = query.ToListAsync(cts.Token);

                    var ae = Assert.Throws<AggregateException>(() => Assert.True(queryEnumerationTask.Wait(1000)));
                    Assert.True(queryEnumerationTask.IsCanceled);					
                    Assert.Equal(1, ae.InnerExceptions.Count);					
                }
            }
        }

        [Fact]
        public async Task WillResultInTheSameResults_Lucene_Async()
        {
            using (GetNewServer())
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8079"
            }.Initialize())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());
                    s.SaveChanges();
                }

                using (var s = store.OpenAsyncSession())
                {
                    var query = s.Advanced.AsyncDocumentQuery<User>();

                    for (int i = 0; i < 5; i++)
                    {
                        var list = await query.ToListAsync();
                        foreach (var user in list)
                        {
                            Assert.NotNull(user.Id);
                        }
                    }
                }
            }
        }
    }
}
