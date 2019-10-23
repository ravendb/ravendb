using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class AutomaticConflictResolvingOnPut : ReplicationTestBase
    {
        public AutomaticConflictResolvingOnPut(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanAutomaticlyResolveConflict()
        {
            using (var master = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var slave = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                await SetupReplicationAsync(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "local"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User
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
                        catch (ConflictException)
                        {
                            session.Store(new User
                            {
                                Name = "I win"
                            }, "users/1");
                            var user = session.Load<User>("users/1");
                            Assert.Equal(user.Name, "I win");
                            return;
                        }
                    }
                    Thread.Sleep(500);
                }

                Assert.True(false, "We were expected to get a conflict on user/1 but we didn't");
            }
        }
    }
}
