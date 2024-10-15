using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;

namespace Raven.Server.Smuggler.Documents
{
    internal sealed class SingleShardDatabaseSmuggler : DatabaseSmuggler
    {
        private readonly int _index;
        private bool _processCompareExchange;
        private ByteStringContext _allocator;
        private readonly ShardingConfiguration _sharding;

        public SingleShardDatabaseSmuggler(ShardedDocumentDatabase database, ISmugglerSource source, ISmugglerDestination destination, SystemTime time,
            JsonOperationContext context, DatabaseSmugglerOptionsServerSide options, SmugglerResult result = null,
            Action<IOperationProgress> onProgress = null, CancellationToken token = default) :
            base(database.ShardedDatabaseName, database, source, destination, time, context, options, result, onProgress, token)
        {
            _sharding = database.ShardingConfiguration;
            _index = ShardHelper.GetShardNumberFromDatabaseName(database.Name);

            Initialize();
        }

        private void Initialize()
        {
            if (_options.IsShard && _index > 0)
                _options.OperateOnTypes &= ~DatabaseSmugglerOptions.OperateOnFirstShardOnly;
            else
                _processCompareExchange = true;
        }

        protected override async Task InternalProcessCompareExchangeAsync(SmugglerResult result, (CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value) kvp,
            ICompareExchangeActions actions)
        {
            if (SkipCompareExchange(kvp.Key))
                return;

            await base.InternalProcessCompareExchangeAsync(result, kvp, actions);
        }

        protected override async Task InternalProcessCompareExchangeTombstonesAsync(SmugglerResult result, (CompareExchangeKey Key, long Index) key, ICompareExchangeActions actions)
        {
            if (SkipCompareExchange(key.Key))
                return;

            await base.InternalProcessCompareExchangeTombstonesAsync(result, key, actions);
        }

        private bool SkipCompareExchange(CompareExchangeKey key)
        {
            if (ClusterWideTransactionHelper.IsAtomicGuardKey(key.Key, out var docId))
            {
                var shardNumber = ShardHelper.GetShardNumberFor(_sharding, _allocator, docId);
                return shardNumber != _index;
            }

            return _processCompareExchange == false;
        }

        public override async Task<SmugglerResult> ExecuteAsync(bool ensureStepsProcessed = true, bool isLastFile = true)
        {
            using (_allocator = new ByteStringContext(SharedMultipleUseFlag.None))
                return await base.ExecuteAsync(ensureStepsProcessed, isLastFile);
        }
    }
}
