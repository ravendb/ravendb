// -----------------------------------------------------------------------
//  <copyright file="AutomaticConflictResolution.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class AutomaticConflictResolution : ReplicationTestBase
    {
        public AutomaticConflictResolution(ITestOutputHelper output) : base(output)
        {
        }


        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ScriptResolveToTombstone(Options options)
        {
            using (var master = GetDocumentStore(options))
            using (var slave = GetDocumentStore(options))
            {

                await SetScriptResolutionAsync(slave, "return resolveToTombstone;", "Users");
                await SetupReplicationAsync(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }

                var tombstoneIDs = WaitUntilHasTombstones(slave);
                Assert.Equal(1, tombstoneIDs.Count);

                using (var session = slave.OpenSession())
                {
                    VerifyRevisionsAfterConflictResolving(session);
                    session.SaveChanges();
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ScriptComplexResolution(Options options)
        {
            using (var master = GetDocumentStore(options))
            using (var slave = GetDocumentStore(options))
            {

                await SetScriptResolutionAsync(slave, @"

function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}

var names = [];
var history = [];
for(var i = 0; i < docs.length; i++) 
{
    names = names.concat(docs[i].Name.split(' '));
    history.push(docs[i]);
}
var out = {
    Name: names.filter(onlyUnique).join(' '),
    Age: Math.max.apply(Math,docs.map(function(o){return o.Age;})),
    Grades:{Bio:12,Math:123,Pys:5,Sports:44},
    Versions:history,
    '@metadata':docs[0]['@metadata']
}
return out;
", "Users");
                await SetupReplicationAsync(master, slave);
                string changeVectorFor;
                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel",
                        Age = 12
                    }, "users/1");
                    session.SaveChanges();
                    changeVectorFor = session.Advanced.GetChangeVectorFor(session.Load<User>("users/1"));
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel",
                        Age = 123
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(WaitForBiggerChangeVector(slave, changeVectorFor));

                using (var session = slave.OpenSession())
                {
                    try
                    {
                        var item = session.Load<User>("users/1");
                        Assert.Equal("Karmel", item.Name);
                        Assert.Equal(123, item.Age);
                        VerifyRevisionsAfterConflictResolving(session);
                    }
                    catch (ConflictException)
                    {
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ScriptUnableToResolve(Options options)
        {
            using (var master = GetDocumentStore(options))
            using (var slave = GetDocumentStore(options))
            {
                await SetupReplicationAsync(master, slave);
                await SetScriptResolutionAsync(slave, @"return;", "Users");

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel1",
                        Age = 1
                    }, "users/1");
                    session.SaveChanges();
                    session.Advanced.GetChangeVectorFor(session.Load<User>("users/1"));
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel2",
                        Age = 2
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.Equal(2, WaitUntilHasConflict(slave, "users/1").Length);
            }
        }

        public bool WaitForBiggerChangeVector(DocumentStore store, string changeVector)
        {
            var sw = Stopwatch.StartNew();

            var timeout = 10000;
            if (Debugger.IsAttached)
                timeout *= 10;
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<User>("users/1");
                    if (ChangeVectorUtils.GetConflictStatus(session.Advanced.GetChangeVectorFor(doc), changeVector) == ConflictStatus.Update)
                        return true;
                }
                Thread.Sleep(10);
            }
            return false;
        }

        public bool WaitForResolution(DocumentStore store)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < 10000)
            {
                using (var session = store.OpenSession())
                {
                    try
                    {
                        session.Load<User>("users/1");
                        return true;
                    }
                    catch
                    {
                        // ignored
                    }
                }
                Thread.Sleep(100);
            }
            return false;
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Should_resolve_conflict_with_scripts(Options options)
        {
            using (var master = GetDocumentStore(options))
            using (var slave = GetDocumentStore(options))
            {

                await SetScriptResolutionAsync(slave, "return {Name:docs[0].Name + '123'};", "Users");
                await SetupReplicationAsync(master, slave);

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var updated = WaitForDocument(slave, "users/1");
                Assert.True(updated);

                using (var session = slave.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }

                var updated2 = WaitForDocument(slave, "users/1");
                Assert.True(updated2);

                using (var session = slave.OpenSession())
                {
                    //this shouldn't throw
                    var item = session.Load<User>("users/1");
                    Assert.Equal(item.Name, "Karmeli123");
                    VerifyRevisionsAfterConflictResolving(session);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldResolveDocumentConflictInFavorOfLatestVersion(Options options)
        {
            using (var master = GetDocumentStore(options))
            using (var slave = GetDocumentStore(options))
            {
                await SetReplicationConflictResolutionAsync(slave, StraightforwardConflictResolution.ResolveToLatest);
                await SetupReplicationAsync(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "1st"
                    }, "users/1");

                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "1st"
                    }, "users/2");

                    session.Store(new
                    {
                        Foo = "marker"
                    }, "marker1$users/2");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(slave, "marker1$users/2"));

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "2nd"
                    }, "users/2");

                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "2nd"
                    }, "users/1");

                    session.Store(new
                    {
                        Foo = "marker"
                    }, "marker2$users/1");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(slave, "marker2$users/1"));

                using (var session = slave.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    var user2 = session.Load<User>("users/2");

                    Assert.Equal("2nd", user1.Name);
                    Assert.Equal("2nd", user2.Name);

                    VerifyRevisionsAfterConflictResolving(session);
                }
            }
        }

        //resolve conflict between incoming document and tombstone
        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Resolve_to_latest_version_tombstone_is_latest_the_incoming_document_is_replicated(Options options)
        {
            using (var master = GetDocumentStore(options))
            using (var slave = GetDocumentStore(options))
            {
                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "1st"
                    }, "users/1");

                    session.SaveChanges();
                }

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "2nd"
                    }, "users/1");

                    session.SaveChanges();
                }

                using (var session = slave.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {

                    session.Store(new
                    {
                        Foo = "marker"
                    }, "marker");

                    session.SaveChanges();
                }

                //the tombstone on the 'slave' node is latest, so after replication finishes,
                //the doc should stay deleted since the replication is 'resolve to latest'
                await SetReplicationConflictResolutionAsync(slave, StraightforwardConflictResolution.ResolveToLatest);
                await SetupReplicationAsync(master, slave);

                Assert.True(WaitForDocument(slave, "marker"));

                using (var session = slave.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Null(user);
                    VerifyRevisionsAfterConflictResolving(session);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ResolveConflictToTombstone(Options options)
        {
            using (var first = GetDocumentStore(options))
            using (var second = GetDocumentStore(options))
            using (var third = GetDocumentStore(options))
            using (var fourth = GetDocumentStore(options))
            {
                using (var session = first.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "1st"
                    }, "users/1");

                    session.SaveChanges();
                }

                using (var session = second.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "2nd"
                    }, "users/1");

                    session.SaveChanges();
                }

                using (var session = second.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                using (var session = first.OpenSession())
                {
                    session.Store(new
                    {
                        Foo = "marker"
                    }, "marker$users/1");

                    session.SaveChanges();
                }

                await SetupReplicationAsync(first, second);
                await SetupReplicationAsync(second, third);
                await SetupReplicationAsync(third, fourth);
                await SetupReplicationAsync(fourth, first);

                Assert.True(WaitForDocument(fourth, "marker$users/1"));

                using (var session = fourth.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Null(user);
                }

                using (var session = fourth.OpenSession())
                {
                    session.Store(new
                    {
                        Foo = "marker"
                    }, "marker2$users/1");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(third, "marker2$users/1"));

                await EnsureNoReplicationLoopAsync(first, options.DatabaseMode);
                await EnsureNoReplicationLoopAsync(second, options.DatabaseMode);
                await EnsureNoReplicationLoopAsync(third, options.DatabaseMode);
                await EnsureNoReplicationLoopAsync(fourth, options.DatabaseMode);
            }
        }

        private static void VerifyRevisionsAfterConflictResolving(IDocumentSession session)
        {
            var revision = session.Advanced.Revisions.GetFor<User>("users/1");
            Assert.Equal(3, revision.Count);

            var metadata = session.Advanced.GetMetadataFor(revision[0]);
            var flags = metadata.GetString(Constants.Documents.Metadata.Flags);
            Assert.Contains(DocumentFlags.Resolved.ToString(), flags);

            metadata = session.Advanced.GetMetadataFor(revision[1]);
            flags = metadata.GetString(Constants.Documents.Metadata.Flags);
            Assert.Contains(DocumentFlags.Conflicted.ToString(), flags);

            metadata = session.Advanced.GetMetadataFor(revision[2]);
            flags = metadata.GetString(Constants.Documents.Metadata.Flags);
            Assert.Contains(DocumentFlags.Conflicted.ToString(), flags);
        }

    }
}
