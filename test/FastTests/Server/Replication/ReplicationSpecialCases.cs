using FastTests.Server.Documents.Replication;
using Raven.Abstractions.Replication;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationSpecialCases : ReplicationTestsBase
    {

        [Fact]
        public async void TomstoneToTombstoneConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }

                var doc = WaitForDocument(slave, "users/1");
                Assert.True(doc);

                using (var session = slave.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                var deletedDoc = WaitForDocumentDeletion(slave, "users/1");
                Assert.True(deletedDoc);

                using (var session = master.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
                bool failed = false;
                try
                {
                    await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                    failed = true;
                }
                catch
                {
                    // all good! no conflict here
                }
                Assert.False(failed);
            }
        }

        [Fact]
        public async void NonIdenticalContentConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }


                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }

                var conflicts = await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                Assert.Equal(2, conflicts["users/1"].Count);
            }
        }

        [Fact]
        public async void NonIdenticalMetadataConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    var user = session.Load<User>("users/1");
                    var meta = session.Advanced.GetMetadataFor(user);
                    meta.Add(("bla"), new RavenJValue("asd"));
                    session.Store(user);
                    session.SaveChanges();
                }


                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    var user = session.Load<User>("users/1");
                    var meta = session.Advanced.GetMetadataFor(user);
                    meta.Add(("bla"), new RavenJValue("asd"));
                    meta.Add(("bla2"), new RavenJValue("asd"));
                    session.SaveChanges();
                }

                var conflicts = await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                Assert.Equal(2, conflicts["users/1"].Count);
            }
        }

        [Fact]
        public async void IdenticalContentConflictResolution()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmel",
                        Age = 12
                    }, "users/1");
                    session.SaveChanges();
                }


                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Age = 12,
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                bool failed = false;
                try
                {
                    await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                    failed = true;
                }
                catch
                {
                    // all good! no conflict here
                }
                Assert.False(failed);
            }
        }

        [Fact]
        public async void UpdateConflictOnParentDocumentArrival()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmeli"
                    }, "users/1");
                    session.SaveChanges();
                }
                var conflicts = await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                Assert.Equal(1, conflicts.Count);

                using (var session = master.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Karmel123"
                    }, "users/1");
                    session.SaveChanges();
                }

                conflicts = await WaitUntilHasConflict(slave, "users/1", 1, 1000);
                Assert.Equal(2, conflicts["users/1"].Count);
            }
        }
    }
}
