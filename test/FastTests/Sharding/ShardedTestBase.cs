using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace FastTests.Sharding
{
    public abstract class ShardedTestBase : RavenTestBase
    {
        protected IDocumentStore GetShardedDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            var name = GetDatabaseName(caller);
            using (var store = new DocumentStore { Urls = new[] { Server.WebUrl } })
            {
                store.Initialize();
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord
                {
                    DatabaseName = name,
                    Shards = new[]
                    {
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                    }
                }));

            }

            DocumentStore documentStore = new DocumentStore
            {
                Urls = new[] { Server.WebUrl },
                Database = name
            };
            documentStore.AfterDispose += (sender, args) =>
            {
                using (var store = new DocumentStore { Urls = new[] { Server.WebUrl } })
                {
                    store.Initialize();
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(name, true));
                }
            };
            return documentStore.Initialize();
        }
    }
}
