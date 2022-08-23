using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.ServerWide;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18561 : ReplicationTestBase
    {
        public RavenDB_18561(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Replication_Doesnt_Work_After_Exception_In_Conflict_Resolver_Script()
        {
            using var storeSrc = GetDocumentStore(new Options { ModifyDatabaseRecord = ModifyDatabaseRecord });
            using var storeDst = GetDocumentStore(new Options { ModifyDatabaseRecord = ModifyDatabaseRecord });

            using (var s1 = storeSrc.OpenSession())
            {
                s1.Store(new User { Name = "test" }, "users/0");
                s1.SaveChanges();
            }
            using (var s2 = storeDst.OpenSession())
            {
                s2.Store(new User { Name = "test2" }, "users/0");
                s2.SaveChanges();
            }

            await SetupReplicationAsync(storeSrc, storeDst);

            using (var s1 = storeSrc.OpenSession())
            {
                s1.Store(new User { Name = "test" }, "users/1");
                s1.SaveChanges();
            }

            Assert.NotNull(WaitForDocumentToReplicate<User>(storeDst, "users/1", 15000));
        }

        void ModifyDatabaseRecord(DatabaseRecord record)
        {
            record.ConflictSolverConfig = new ConflictSolver
            {
                ResolveToLatest = false,
                ResolveByCollection = new Dictionary<string, ScriptResolver>
                {
                    {"Users", new ScriptResolver {Script = "throw new Error('Something is wrong!!!');"}}
                }
            };
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
