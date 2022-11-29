using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents;
using Raven.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.Documents.Operations.Replication;
using System.Threading;
using System.Collections.Concurrent;
using System.IO;
using Lucene.Net.Util;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using FastTests.Utils;
using Sparrow;

namespace SlowTests.Issues
{
    public class RavenDB_19561 : ReplicationTestBase
    {
        public RavenDB_19561(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Replicate_2_Docs_Which_Is_Bigger_Then_Batch_Size()
        {
            var co = new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Replication.MaxSizeToSend)] = 1.ToString()
                }
            };
            var node = GetNewServer(co);

            using var store1 = GetDocumentStore(new Options { RunInMemory = false, Server = node, ReplicationFactor = 1 });
            using var store2 = GetDocumentStore(new Options { RunInMemory = false, Server = node, ReplicationFactor = 1 });

            var docs = new List<User>();
            using (var session = store1.OpenAsyncSession())
            {
                var doc = new User
                {
                    Id = $"Users/1-A",
                    Name = $"User1",
                    Info = GenRandomString(2_000_000)
                };
                docs.Add(doc);
                await session.StoreAsync(doc);
                await session.SaveChangesAsync();
            }

            using (var session = store1.OpenAsyncSession())
            {
                var doc = new User
                {
                    Id = $"Users/2-A",
                    Name = $"User2",
                    Info = GenRandomString(2_000_000)
                };
                docs.Add(doc);
                await session.StoreAsync(doc);
                await session.SaveChangesAsync();
            }

            var externalList = await SetupReplicationAsync(store1, store2);

            // wait for replication from store1 to store2/3
            var waitForReplicationTasks = new List<Task>();
            foreach (var doc in docs)
            {
                waitForReplicationTasks.Add(WaitAndAssertDocReplicationAsync<User>(store2, doc.Id));
            }
            Task.WaitAll(waitForReplicationTasks.ToArray());

        }

        private string GenRandomString(int size)
        {
            var sb = new StringBuilder(size);
            var ran = new Random();
            var firstCharAsInt = Convert.ToInt32('a');
            var lastCharAsInt = Convert.ToInt32('z');
            for (int i = 0; i < size; i++)
            {
                sb.Append(Convert.ToChar(ran.Next(firstCharAsInt, lastCharAsInt + 1)));
            }

            return sb.ToString();
        }


        private async Task WaitAndAssertDocReplicationAsync<T>(DocumentStore store, string id, int timeout = 15_000_000) where T : class
        {
            var result = await WaitForDocumentToReplicateAsync<User>(store, id, timeout);
            Assert.True(result!=null, $"doc \"{id}\" didn't replicated.");
        }

        class User
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public string Info { get; set; }
        }

    }
}
