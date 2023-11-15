using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20955 : ReplicationTestBase
    {
        public RavenDB_20955(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task OnChangeShouldTrigger_waitForChangesForExternalReplication()
        {
            var sw = new Stopwatch();
            long timeElapsed1;
            long timeElapsed2;

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            using (var store3 = GetDocumentStore())
            {
                sw.Start();

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store3);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();

                    var user = session.Load<User>("users/1");
                    user.Name = "Shiran";
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(store3, "users/1", u => u.Name == "Shiran"));

                sw.Stop();
                timeElapsed2 = sw.ElapsedMilliseconds;
            }

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            using (var store3 = GetDocumentStore())
            {
                var db2 = await GetDatabase(store2.Database);
                db2.ReplicationLoader.ForTestingPurposesOnly().OnOutgoingReplicationStart = (o) =>
                {
                    if (o.Destination.Database == store3.Database)
                        o.ForTestingPurposesOnly().DisableWaitForChangesForExternalReplication = true;
                };

                sw.Restart();

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store3);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();

                    var user = session.Load<User>("users/1");
                    user.Name = "Shiran";
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(store3, "users/1", u => u.Name == "Shiran"));
              
                sw.Stop();
                timeElapsed1 = sw.ElapsedMilliseconds;
            }

            Assert.True(timeElapsed2 < timeElapsed1);
        }
    }
}
