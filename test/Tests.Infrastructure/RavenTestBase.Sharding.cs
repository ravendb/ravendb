using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Utils;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly ShardingTestBase Sharding;

    public class ShardingTestBase
    {
        private readonly RavenTestBase _parent;

        public ShardingTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public IDocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            var shardedOptions = options ?? new Options();
            var old = shardedOptions.ModifyDatabaseRecord;
            shardedOptions.ModifyDatabaseRecord = r =>
            {
                r.Sharding = new ShardingRecord
                {
                    Shards = new[]
                    {
                        new DatabaseTopology(),
                        new DatabaseTopology(), 
                        new DatabaseTopology(),
                    }
                };
                old.Invoke(r);
            };
            shardedOptions.ModifyDocumentStore = s => s.Conventions.OperationStatusFetchMode = OperationStatusFetchMode.Polling;
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "remove above after changes api is working");
            return _parent.GetDocumentStore(shardedOptions, caller);
        }

        public async Task<int> GetShardNumber(IDocumentStore store, string id)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            return ShardHelper.GetShardNumberFor(record.Sharding, id);
        }

        public async Task<IEnumerable<DocumentDatabase>> GetShardsDocumentDatabaseInstancesFor(IDocumentStore store, string database = null)
        {
            var dbs = new List<DocumentDatabase>();
            foreach (var task in _parent.Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database ?? store.Database))
            {
                dbs.Add(await task);
            }

            return dbs;
        }

        public bool AllShardHaveDocs(IDictionary<string, List<DocumentDatabase>> servers, long count = 1L)
        {
            foreach (var kvp in servers)
            {
                foreach (var documentDatabase in kvp.Value)
                {
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var ids = documentDatabase.DocumentsStorage.GetNumberOfDocuments(context);
                        if (ids < count)
                            return false;
                    }
                }
            }

            return true;
        }
    }
}

