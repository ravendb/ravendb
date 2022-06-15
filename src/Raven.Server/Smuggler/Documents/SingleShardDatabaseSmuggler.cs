using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    internal class SingleShardDatabaseSmuggler : DatabaseSmuggler
    {
        private readonly TransactionContextPool _serverContextPool;
        private readonly DatabaseRecord _shardedRecord;
        private readonly int _index;
        private bool _processCompareExchange;

        public SingleShardDatabaseSmuggler(DocumentDatabase database, ISmugglerSource source, ISmugglerDestination destination, SystemTime time, 
            JsonOperationContext context, DatabaseSmugglerOptionsServerSide options, SmugglerResult result = null, 
            Action<IOperationProgress> onProgress = null, CancellationToken token = default) : 
            base(database, source, destination, time, context, options, result, onProgress, token)
        {
            _serverContextPool = database.ServerStore.ContextPool;
            _shardedRecord = _source.GetShardedDatabaseRecordAsync().Result;
            _index = ShardHelper.GetShardNumber(database.Name);
    
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
            if (ClusterTransactionCommand.IsAtomicGuardKey(key.Key, out var docId))
            {
                using (_serverContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    var bucket = ShardHelper.GetBucket(context, docId);
                    var shardNumber = ShardHelper.GetShardNumber(_shardedRecord.Sharding.ShardBucketRanges, bucket);

                    return shardNumber != _index;
                }
            }
            return _processCompareExchange == false;
        }
    }
}
