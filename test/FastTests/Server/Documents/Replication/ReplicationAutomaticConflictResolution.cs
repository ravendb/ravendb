// -----------------------------------------------------------------------
//  <copyright file="AutomaticConflictResolution.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using Raven.Abstractions.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class AutomaticConflictResolution : ReplicationTestsBase
    {
        [Fact]
        public void ShouldResolveDocumentConflictInFavorOfLocalVersion()
        {
            DocumentConflictResolveTest(StraightforwardConflictResolution.ResolveToLocal);
        }

        [Fact]
        public void ShouldResolveDocumentConflictInFavorOfRemoteVersion()
        {
            DocumentConflictResolveTest(StraightforwardConflictResolution.ResolveToRemote);
        }

        [Fact]
        public void ShouldResolveDocumentConflictInFavorOfLatestVersion()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                SetupReplication(master, slave);
                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.ResolveToLatest);
                using (var session = slave.OpenSession())
                {
                
                    session.Store(new User()
                    {
                        Name = "1st"
                    }, "users/1");

                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "1st"
                    }, "users/2");

                    session.SaveChanges();
                }

                Thread.Sleep(2000);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "2nd"
                    }, "users/2");

                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "2nd"
                    }, "users/1");

                    session.Store(new
                    {
                        Foo = "marker"
                    }, "marker");

                    session.SaveChanges();
                }

                var marker = WaitForDocument(slave, "marker");

                Assert.NotNull(marker);

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
        public void Resolve_to_latest_version_tombstone_is_latest_the_incoming_document_is_replicated()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "1st"
                    }, "users/1");

                    session.SaveChanges();
                }

                Thread.Sleep(1000);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "2nd"
                    }, "users/1");

                    session.SaveChanges();
                }

                Thread.Sleep(1000);

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
                SetupReplication(master, slave);
                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.ResolveToLatest);

                var marker = WaitForDocument(slave, "marker");

                Assert.NotNull(marker);

                using (var session = slave.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Equal("1st", user.Name);
                }
            }
        }


        private void DocumentConflictResolveTest(StraightforwardConflictResolution docConflictResolution)
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                SetupReplication(master, slave);
                SetReplicationConflictResolution(slave, docConflictResolution);

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

                    session.Store(new
                    {
                        Foo = "marker"
                    }, "marker");

                    session.SaveChanges();
                }

                var marker = WaitForDocument(slave, "marker");

                Assert.NotNull(marker);

                using (var session = slave.OpenSession())
                {
                    var item = session.Load<User>("users/1");

                    switch (docConflictResolution)
                    {
                        case StraightforwardConflictResolution.ResolveToLocal:
                            Assert.Equal("local", item?.Name);
                            break;
                        case StraightforwardConflictResolution.ResolveToRemote:
                            Assert.Equal("remote", item?.Name);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(docConflictResolution));
                    }
                }
            }
        }

        
    }
}