using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15900 : ReplicationTestBase
    {
        public RavenDB_15900(ITestOutputHelper output) : base(output)
        {
        }

        public class TestCommandValidator : RachisVersionValidation
        {
            public override void AssertPutCommandToLeader(CommandBase cmd)
            {
            }

            public override void AssertEntryBeforeSendToFollower(BlittableJsonReaderObject entry, int version, string follower)
            {
            }
        }

        [Fact]
        public async Task RemoveEntryFromRaftLogTest()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var database = GetDatabaseName();

            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);
            using (var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                leader.ServerStore.Engine.StateMachine.Validator = new TestCommandValidator();
                var documentDatabase = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);

                var cmd = new RachisConsensusTestBase.TestCommandWithRaftId("test", RaftIdGenerator.NewId());

                _ = leader.ServerStore.Engine.CurrentLeader.PutAsync(cmd, new TimeSpan(2000));

                var cmd2 = new CreateDatabaseOperation.CreateDatabaseCommand(new DatabaseRecord("Toli"), 1);

                _ = leader.ServerStore.SendToLeaderAsync(new AddDatabaseCommand(Guid.NewGuid().ToString())
                {
                    Record = new DatabaseRecord("Toli")
                    {
                        Topology = new DatabaseTopology
                        {
                            Members = new List<string> { "A", "B", "C" },
                            Rehabs = new List<string> { },
                            ReplicationFactor = 3
                        }
                    },
                    Name = "Toli"

                });

                foreach (var server in Servers)
                {
                    Assert.False(leader.ServerStore.DatabasesLandlord.IsDatabaseLoaded("Toli"));
                }

                foreach (var server in Servers)
                {
                    long index = 0;
                    documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    using (documentDatabase.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        server.ServerStore.Engine.GetLastCommitIndex(context, out index, out long term);
                    }

                    server.ServerStore.Engine.RemoveEntryFromRaftLog(index + 1);
                }
                foreach (var server in Servers)
                {
                    var val = WaitForValueAsync(() => server.ServerStore.DatabasesLandlord.IsDatabaseLoaded("Toli"), true);
                    Assert.True(val.Result);
                }
            }
        }
        internal class TestCommandWithRaftId : CommandBase
        {
            private string Name;

#pragma warning disable 649
            private object Value;
#pragma warning restore 649

            public TestCommandWithRaftId(string name, string uniqueRequestId) : base(uniqueRequestId)
            {
                Name = name;
            }

            public override DynamicJsonValue ToJson(JsonOperationContext context)
            {
                var djv = base.ToJson(context);
                djv[nameof(Name)] = Name;
                djv[nameof(Value)] = Value;

                return djv;
            }
        }
    }
}
