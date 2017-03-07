using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Server.Operations;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationWithFailover : ReplicationTestsBase
    {
        public class User
        {
            public string Name;
        }

        [Fact]
        public async Task LoadDocumentsWithFailOver()
        {
            var master = GetDocumentStore(ignoreDisabledDatabase: true);
            var slave = GetDocumentStore(ignoreDisabledDatabase: true);

            try
            {

                SetupReplication(master, slave);
                EnsureReplicating(master, slave);

                var sp = Stopwatch.StartNew();
                var requestExecutor = master.GetRequestExecuter();

                while (await requestExecutor.UpdateTopology() == false)
                {
                    Assert.False(sp.Elapsed.Seconds > 30);
                }

                using (var session = master.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges();
                    session.Store(new User { Name = "Idan" }, "users/1");
                    session.Store(new User { Name = "Shalom" }, "users/2");
                    session.SaveChanges();
                }

                var result = master.Admin.Server.Send(new DisableResourceToggleOperation(master.DefaultDatabase, true));
                Assert.True(result.Disabled);
                Assert.Equal(master.DefaultDatabase, result.Name);

                using (var session = master.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    Assert.NotNull(user1);
                    Assert.Equal("Idan", user1.Name);
                    var user2 = session.Load<User>("users/2");
                    Assert.NotNull(user2);
                    Assert.Equal("Shalom", user2.Name);
                }

            }
            finally
            {
                master.Dispose();
                slave.Dispose();
            }
        }
    }
}
