// -----------------------------------------------------------------------
//  <copyright file="AutomaticConflictResolution.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class AutomaticConflictResolution : ReplicationTestsBase
    {
        [Fact]
        public async Task ScriptResolveToTombstone()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                await SetScriptResolutionAsync(slave, "return ResolveToTombstone();", "Users");
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
            }
        }

        [Fact]
        public async Task ScriptComplexResolution()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
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
output(out);
return out;
", "Users");
                await SetupReplicationAsync(master, slave);
                long? etag;
                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel",
                        Age = 12
                    }, "users/1");
                    session.SaveChanges();
                    etag = session.Advanced.GetEtagFor(session.Load<User>("users/1"));
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

                Assert.True(WaitForBiggerEtag(slave, etag));

                using (var session = slave.OpenSession())
                {
                    try
                    {
                        var item = session.Load<User>("users/1");
                        Assert.Equal("Karmel", item.Name);
                        Assert.Equal(123, item.Age);
                    }
                    catch (ConflictException)
                    {
                    }
                }
            }
        }

        [Fact]
        public async Task ScriptUnableToResolve()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                await SetupReplicationAsync(master, slave);
                await SetScriptResolutionAsync(slave, @"return;", "Users");

                long? etag;
                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel1",
                        Age = 1
                    }, "users/1");
                    session.SaveChanges();
                    etag = session.Advanced.GetEtagFor(session.Load<User>("users/1"));
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

                Assert.Equal(2, WaitUntilHasConflict(slave, "users/1").Results.Length);
            }
        }

        public bool WaitForBiggerEtag(DocumentStore store, long? etag)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < 10000)
            {
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<User>("users/1");
                    if (session.Advanced.GetEtagFor(doc) > etag)
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

        [Fact]
        public async Task Should_resolve_conflict_with_scripts()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
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
                }
            }
        }

        [Fact]
        public async Task ShouldResolveDocumentConflictInFavorOfLatestVersion()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
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
                    }, "marker1");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(slave, "marker1"));

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
                    }, "marker2");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(slave, "marker2"));

                using (var session = slave.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    var user2 = session.Load<User>("users/2");

                    Assert.Equal("2nd", user1.Name);
                    Assert.Equal("2nd", user2.Name);
                }
            }
        }

        //resolve conflict between incoming document and tombstone
        [Fact]
        public async Task Resolve_to_latest_version_tombstone_is_latest_the_incoming_document_is_replicated()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
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
                    //Assert.Equal("1st", user.Name);
                }
            }
        }
    }
}