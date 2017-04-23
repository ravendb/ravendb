using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationSpecialCasesSlow : ReplicationTestsBase
    {
        [Fact]
        public async Task IdenticalContentConflictResolution()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                await SetReplicationConflictResolutionAsync(slave, StraightforwardConflictResolution.None);
                await SetupReplicationAsync(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel",
                        Age = 12
                    }, "users/1");
                    session.SaveChanges();
                }


                using (var session = master.OpenSession())
                {
                    session.Store(new User
                    {
                        Age = 12,
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                bool failed = false;
                try
                {
                    WaitUntilHasConflict(slave, "users/1", 1);
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
        public async Task TomstoneToTombstoneConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {

                await SetReplicationConflictResolutionAsync(slave, StraightforwardConflictResolution.None);
                await SetupReplicationAsync(master, slave);

                using (var session = master.OpenSession())
                {
                    session.Store(new User
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
                    WaitUntilHasConflict(slave, "users/1", 1);
                    failed = true;
                }
                catch
                {
                    // all good! no conflict here
                }
                Assert.False(failed);
            }
        }

    }
}