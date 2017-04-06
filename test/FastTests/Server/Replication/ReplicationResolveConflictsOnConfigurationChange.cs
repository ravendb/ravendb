using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationResolveConflictsOnConfigurationChange : ReplicationTestsBase
    {

        public void GenerateConflicts(DocumentStore store1, DocumentStore store2, string id = "foo/bar")
        {
            using (var session = store1.OpenSession())
            {
                session.Store(new User
                {
                    Name = "Store1"
                }, id);
                session.SaveChanges();
            }
            using (var session = store2.OpenSession())
            {
                session.Store(new User
                {
                    Name = "Store2"
                }, id);
                session.SaveChanges();
            }
            SetupReplication(store1,store2);
            SetupReplication(store2,store1);

            Assert.Equal(2, WaitUntilHasConflict(store1, id).Results.Length);
            Assert.Equal(2, WaitUntilHasConflict(store2, id).Results.Length);
        }

        [Fact]
        public void ResolveWhenScriptAdded()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                GenerateConflicts(store1, store2);
                var config = new ConflictSolver
                {
                    ResolveByCollection = new Dictionary<string, ScriptResolver>
                    {
                        {
                            "Users", new ScriptResolver
                            {
                                Script = "return {'Name':'Resolved'}"
                            }
                        }
                    }
                };
                SetupReplication(store1, config, store2);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Resolved"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Resolved"));
            }
        }

        [Fact]
        public void ResolveWhenChangeToLatest()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                GenerateConflicts(store1, store2);

                SetReplicationConflictResolution(store1, StraightforwardConflictResolution.ResolveToLatest);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Store2"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Store2"));
            }
        }

        [Fact]
        public void ResolveWhenSettingDatabaseResolver()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                GenerateConflicts(store1, store2);
                var config = new ConflictSolver
                {
                    DatabaseResovlerId = GetDocumentDatabaseInstanceFor(store1).Result.DbId.ToString()
                };
                SetupReplication(store1, config, store2);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Store1"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Store1"));
            }
        }

        [Fact]
        public async Task ResolveManyConflicts()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                GenerateConflicts(store1, store2, "users/1");
                await SetupReplicationAsync(store1);
                await SetupReplicationAsync(store2);
                GenerateConflicts(store1, store2, "users/2");
                
                await UpdateConflictResolver(store1, GetDocumentDatabaseInstanceFor(store1).Result.DbId.ToString());

                Assert.True(WaitForDocument<User>(store1, "users/1", u => u.Name == "Store1"));
                Assert.True(WaitForDocument<User>(store2, "users/1", u => u.Name == "Store1"));
                Assert.True(WaitForDocument<User>(store1, "users/2", u => u.Name == "Store1"));
                Assert.True(WaitForDocument<User>(store2, "users/2", u => u.Name == "Store1"));
            }
        }
    }
}
