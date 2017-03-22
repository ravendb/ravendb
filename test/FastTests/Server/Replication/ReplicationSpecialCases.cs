using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationSpecialCases : ReplicationTestsBase
    {

        [Fact]
        public void NonIdenticalContentConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

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

                var conflicts = WaitUntilHasConflict(slave, "users/1", 1);
                Assert.Equal(2, conflicts["users/1"].Count);
            }
        }

        [Fact]
        public void NonIdenticalMetadataConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    var user = session.Load<User>("users/1");
                    var meta = session.Advanced.GetMetadataFor(user);
                    meta.Add("bla", "asd");
                    session.Store(user);
                    session.SaveChanges();
                }


                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    var user = session.Load<User>("users/1");
                    var meta = session.Advanced.GetMetadataFor(user);
                    meta.Add("bla", "asd");
                    meta.Add("bla2", "asd");
                    session.SaveChanges();
                }

                var conflicts = WaitUntilHasConflict(slave, "users/1", 1);
                Assert.Equal(2, conflicts["users/1"].Count);
            }
        }


        [Fact]
        public void UpdateConflictOnParentDocumentArrival()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                SetReplicationConflictResolution(slave, StraightforwardConflictResolution.None);
                SetupReplication(master, slave);

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
                var conflicts = WaitUntilHasConflict(slave, "users/1", 1);
                Assert.Equal(1, conflicts.Count);

                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel123"
                    }, "users/1");
                    session.SaveChanges();
                }

                conflicts = WaitUntilHasConflict(slave, "users/1", 1);
                Assert.Equal(2, conflicts["users/1"].Count);
            }
        }
    }
}
