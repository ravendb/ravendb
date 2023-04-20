using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Replication;
using Raven.Client.Exceptions.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.NotificationCenter;
using Raven.Server.Utils;
using Xunit;
using Constants = Raven.Client.Constants;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Xunit.Abstractions;
using System.Threading;
using Amazon.Runtime;
using System.Security.Cryptography;
using Nito.AsyncEx;
using Tests.Infrastructure;
using static Raven.Server.Documents.Replication.ReplicationOperation;

namespace SlowTests.Issues
{
    public class RavenDB_18916 : ReplicationTestBase
    {
        public RavenDB_18916(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DeadLockTest()
        {
            var server = GetNewServer();

            using var store1 = GetDocumentStore(new Options { Server = server, ReplicationFactor = 1 });
            using var store2 = GetDocumentStore(new Options { Server = server, ReplicationFactor = 1 });

            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store2.Database);
            var replicationLoader = database.ReplicationLoader;

            var handlersMre = new ManualResetEvent(false);
            replicationLoader.ForTestingPurposesOnly().OnIncomingReplicationHandlerStart = () =>
            {
                throw new EndOfStreamException("Shahar");
            };
            replicationLoader.ForTestingPurposesOnly().OnIncomingReplicationHandlerFailure = (e) =>
            {
                handlersMre.WaitOne();
            };
            replicationLoader.ForTestingPurposesOnly().BeforeDisposingIncomingReplicationHandlers = () =>
            {
                handlersMre.Set();
            };

            await SetupReplicationAsync(store1, store2);

            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = "Users/2-A", Name = "Shahar" });
                await session.SaveChangesAsync();
            }

            await DisposeServerAsync(server, 20_000); // Shouldnt throw "System.InvalidOperationException: Could not dispose server with URL.."
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
