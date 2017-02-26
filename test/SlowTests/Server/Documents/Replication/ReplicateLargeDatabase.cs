using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Http;
using Raven.Server.Documents.Replication;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Server.Documents.Replication
{
    public class ReplicateLargeDatabase : ReplicationTestsBase
    {
        [Fact]
        public void AutomaticResolveWithIdenticalContent()
        {
            DocumentStore store1;
            DocumentStore store2;

            CreateSampleDatabase(out store1);
            CreateSampleDatabase(out store2);

            SetupReplication(store1,store2);
            Assert.Equal(1,WaitForValue(()=>GetReplicationStats(store2).IncomingStats.Count,1));
            var stats = GetReplicationStats(store2);
            Assert.True(stats.IncomingStats.Any(o =>
            {
                var stat = (ReplicationStatistics.IncomingBatchStats)o;
                if (stat.Exception != null)
                {
                    throw new InvalidDataException(stat.Exception);
                }
                return true;
            }));
        }

        public void CreateSampleDatabase(out DocumentStore store)
        {
            store = GetDocumentStore();
            CallCreateSampleDatabaseEndpoint(store);
        }

        public bool CallCreateSampleDatabaseEndpoint(DocumentStore store)
        {
            using (var commands = store.Commands())
            {
                var command = new CreateSampleDatabaseEndpoint();

                commands.RequestExecuter.Execute(command, commands.Context);

                return command.Result;
            }
        }

        private class CreateSampleDatabaseEndpoint : RavenCommand<bool>
        {
            public override bool IsReadRequest => true;
            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/studio/sample-data";

                ResponseType = RavenCommandResponseType.Object;
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = true;
            }
        }

    }
}
