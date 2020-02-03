using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class RavenDB_14348 : RavenTestBase
    {
        public RavenDB_14348(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var task = Task.Run(async () =>
                {
                    while (false == store.WasDisposed)
                    {
                        try
                        {
                            var ids = new List<string>();
                            using (var session = store.OpenSession(new SessionOptions
                            {
                                TransactionMode = TransactionMode.ClusterWide
                            }))
                            {
                                for (var k = 0; k < 10; k++)
                                {
                                    User entity = new User
                                    {
                                        Name = "ClusteredJohnny" + k
                                    };
                                    session.Store(entity);
                                    ids.Add(session.Advanced.GetDocumentId(entity));
                                }
                                session.SaveChanges();
                            }

                            using (var session = store.OpenSession())
                            {
                                for (var k = 0; k < 10; k++)
                                {
                                    session.Store(new User
                                    {
                                        Name = "Johnny" + k
                                    });
                                }
                                session.SaveChanges();
                            }

                            using (var session = store.OpenSession())
                            {
                                for (var k = 0; k < 10; k++)
                                {
                                    var user = session.Load<User>(ids[k]);
                                    user.Age++;
                                }
                                session.SaveChanges();
                            }
                            await Task.Delay(16);
                        }
                        catch (AllTopologyNodesDownException)
                        {
                        }
                        catch (DatabaseDisabledException)
                        {
                        }
                        catch (DatabaseDoesNotExistException)
                        {
                        }
                        catch (RavenException)
                        {
                        }
                    }
                });

                await Task.Delay(5555);
            }
        }
    }
}
