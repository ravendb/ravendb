using System;
using System.Net;
using System.Threading;
using Raven.Abstractions.Connection;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class AutomaticConflictResolvingOnPut : ReplicationTestsBase
    {
        [Fact]
        public void CanAutomaticlyResolveConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "local"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "remote"
                    }, "users/1");
                    session.SaveChanges();
                }
                var startTime = DateTime.UtcNow;
                while ((DateTime.UtcNow - startTime).TotalSeconds < 10)
                {
                    using (var session = slave.OpenSession())
                    {
                        try
                        {
                            var item = session.Load<User>("users/1");
                        }
                        catch (ErrorResponseException e)
                        {
                            Assert.Equal(HttpStatusCode.Conflict, e.StatusCode);
                            session.Store(new User()
                            {
                                Name = "I win"
                            }, "users/1");
                            var user = session.Load<User>("users/1");
                            Assert.Equal(user.Name, "I win");
                            return;
                        }
                    }
                    Thread.Sleep(500); ;
                }

                Assert.True(false, "We were expected to get a conflict on user/1 but we didn't");
            }
        }
    }
}
