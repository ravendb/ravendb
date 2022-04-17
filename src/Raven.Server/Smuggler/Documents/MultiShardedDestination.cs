using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;

namespace Raven.Server.Smuggler.Documents
{
    public class MultiShardedDestination : ISmugglerDestination
    {
        private readonly ShardedDatabaseContext _databaseContext;
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly ISmugglerSource _source;
        private readonly StreamDestination[] _destinations;
        private DatabaseSmugglerOptionsServerSide _options;

        public MultiShardedDestination(ISmugglerSource source, ShardedDatabaseContext databaseContext, ShardedDatabaseRequestHandler handler)
        {
            _source = source;
            _databaseContext = databaseContext;
            _handler = handler;
            _destinations = new StreamDestination[databaseContext.ShardCount];
        }

        public async ValueTask<IAsyncDisposable> InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion)
        {
            _options = options;
            var holders = new StreamDestinationHolder[_databaseContext.ShardCount];

            var importOperation = new ShardedImportOperation(_handler, holders, options);
            var t = _databaseContext.ShardExecutor.ExecuteParallelForAllAsync(importOperation);

            await Task.WhenAll(importOperation.ExposedStreamTasks);

            for (int i = 0; i < holders.Length; i++)
            {
                await PrepareShardStreamDestination(holders, i, result, buildVersion);
            }

            return new AsyncDisposableAction(async () =>
            {
                for (int i = 0; i < holders.Length; i++)
                {
                    await holders[i].DisposeAsync();
                }

                await t;
            });
        }

        private async Task PrepareShardStreamDestination(StreamDestinationHolder[] holders, int shard, SmugglerResult result, long buildVersion)
        {
            var stream = _handler.GetOutputStream(holders[shard].OutStream.OutputStream.Result, _options);
            holders[shard].InputStream = stream;
            holders[shard].ContextReturn = _handler.ContextPool.AllocateOperationContext(out JsonOperationContext context);
            var destination = new StreamDestination(stream, context, _source);
            holders[shard].DestinationAsyncDisposable = await destination.InitializeAsync(_options, result, buildVersion);
            _destinations[shard] = destination;
        }

        internal struct StreamDestinationHolder : IAsyncDisposable
        {
            public Stream InputStream;
            public StreamExposerContent OutStream;
            public IDisposable ContextReturn;
            public IAsyncDisposable DestinationAsyncDisposable;

            public async ValueTask DisposeAsync()
            {
                await DestinationAsyncDisposable.DisposeAsync();
                OutStream.Complete();
                ContextReturn.Dispose();
            }
        }

        // All the NotImplementedException methods are handled on the smuggler level, since they are cluster wide and do no require any specific database
        public IDatabaseRecordActions DatabaseRecord() => throw new NotImplementedException();
        public IIndexActions Indexes() => throw new NotImplementedException();
        public IKeyValueActions<long> Identities() => throw new NotImplementedException();
        public ISubscriptionActions Subscriptions() => throw new NotImplementedException();
        public IReplicationHubCertificateActions ReplicationHubCertificates() => throw new NotImplementedException();

        public ICompareExchangeActions CompareExchange(JsonOperationContext context) =>
            new ShardedCompareExchangeActions(_databaseContext, _destinations.Select(d => d.CompareExchange(context)).ToArray(), _options);

        public ICompareExchangeActions CompareExchangeTombstones(JsonOperationContext context) =>
            new ShardedCompareExchangeActions(_databaseContext, _destinations.Select(d => d.CompareExchange(context)).ToArray(), _options);

        public IDocumentActions Documents(bool throwOnCollectionMismatchError = true) =>
            new SharededDocumentActions(_databaseContext, _destinations.Select(d => d.Documents(throwOnDuplicateCollection: false)).ToArray(), _options);

        public IDocumentActions RevisionDocuments() =>
            new SharededDocumentActions(_databaseContext, _destinations.Select(d => d.RevisionDocuments()).ToArray(), _options);

        public IDocumentActions Tombstones() =>
            new SharededDocumentActions(_databaseContext, _destinations.Select(d => d.Tombstones()).ToArray(), _options);

        public IDocumentActions Conflicts() =>
            new SharededDocumentActions(_databaseContext, _destinations.Select(d => d.Conflicts()).ToArray(), _options);

        public ICounterActions Counters(SmugglerResult result) =>
            new ShardedCounterActions(_databaseContext, _destinations.Select(d => d.Counters(result)).ToArray(), _options);

        public ICounterActions LegacyCounters(SmugglerResult result) =>
            new ShardedCounterActions(_databaseContext, _destinations.Select(d => d.LegacyCounters(result)).ToArray(), _options);

        public ITimeSeriesActions TimeSeries() =>
            new ShardedTimeSeriesActions(_databaseContext, _destinations.Select(d => d.TimeSeries()).ToArray(), _options);

        public ILegacyActions LegacyDocumentDeletions() =>
            new ShardedLegacyActions(_databaseContext, _destinations.Select(d => d.LegacyDocumentDeletions()).ToArray(), _options);

        public ILegacyActions LegacyAttachmentDeletions() =>
            new ShardedLegacyActions(_databaseContext, _destinations.Select(d => d.LegacyAttachmentDeletions()).ToArray(), _options);


        private abstract class ShardedActions<T> : INewDocumentActions, INewCompareExchangeActions where T : IAsyncDisposable
        {
            private JsonOperationContext _context;
            private readonly IDisposable _rtnCtx;
            private readonly DatabaseSmugglerOptionsServerSide _options;
            protected readonly ShardedDatabaseContext DatabaseContext;
            protected readonly T[] _actions;
            protected readonly T _last;

            protected ShardedActions(ShardedDatabaseContext databaseContext, T[] actions, DatabaseSmugglerOptionsServerSide options)
            {
                DatabaseContext = databaseContext;
                _actions = actions;
                _last = _actions.Last();
                _options = options;
                _rtnCtx = DatabaseContext.AllocateContext(out _context);
            }

            public JsonOperationContext GetContextForNewCompareExchangeValue() => _context;
            public JsonOperationContext GetContextForNewDocument() => _context;

            public virtual async ValueTask DisposeAsync()
            {
                foreach (var action in _actions)
                {
                    await action.DisposeAsync();
                }

                _rtnCtx.Dispose();
            }

            public Stream GetTempStream() => StreamDestination.GetTempStream(_options);
        }

        private class ShardedCompareExchangeActions : ShardedActions<ICompareExchangeActions>, ICompareExchangeActions
        {
            public ShardedCompareExchangeActions(ShardedDatabaseContext databaseContext, ICompareExchangeActions[] actions, DatabaseSmugglerOptionsServerSide options) : base(databaseContext, actions, options)
            {
            }

            public async ValueTask WriteKeyValueAsync(string key, BlittableJsonReaderObject value)
            {
                if (ClusterTransactionCommand.IsAtomicGuardKey(key, out var docId))
                {
                    var shardNumber = DatabaseContext.GetShardNumberFor(key);
                    await _actions[shardNumber].WriteKeyValueAsync(key, value);
                    return;
                }
               
                await _last.WriteKeyValueAsync(key, value);
            }

            public async ValueTask WriteTombstoneKeyAsync(string key)
            {
                if (ClusterTransactionCommand.IsAtomicGuardKey(key, out var docId))
                {
                    var shardNumber = DatabaseContext.GetShardNumberFor(key);;
                    await _actions[shardNumber].WriteTombstoneKeyAsync(key);
                    return;
                }

                await _last.WriteTombstoneKeyAsync(key);
            }
        }


        private class SharededDocumentActions : ShardedActions<IDocumentActions>, IDocumentActions
        {
            private readonly ByteStringContext _allocator;

            public SharededDocumentActions(ShardedDatabaseContext databaseContext, IDocumentActions[] actions, DatabaseSmugglerOptionsServerSide options) : base(databaseContext, actions, options)
            {
                _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            }

            public override async ValueTask DisposeAsync()
            {
                await base.DisposeAsync();
                _allocator.Dispose();
            }

            public async ValueTask WriteDocumentAsync(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, item.Document.Id);
                await _actions[shardNumber].WriteDocumentAsync(item, progress);
            }

            public async ValueTask WriteTombstoneAsync(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, tombstone.LowerId);
                await _actions[shardNumber].WriteTombstoneAsync(tombstone, progress);
            }

            public async ValueTask WriteConflictAsync(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, conflict.Id);
                await _actions[shardNumber].WriteConflictAsync(conflict, progress);
            }

            public ValueTask DeleteDocumentAsync(string id) => ValueTask.CompletedTask;

            public IEnumerable<DocumentItem> GetDocumentsWithDuplicateCollection()
            {
                yield break;
            }
        }

        private class ShardedCounterActions : ShardedActions<ICounterActions>, ICounterActions
        {
            private readonly ByteStringContext _allocator;

            public ShardedCounterActions(ShardedDatabaseContext databaseContext, ICounterActions[] actions, DatabaseSmugglerOptionsServerSide options) : base(databaseContext, actions, options)
            {
                _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            }

            public override async ValueTask DisposeAsync()
            {
                await base.DisposeAsync();
                _allocator.Dispose();
            }

            public async ValueTask WriteCounterAsync(CounterGroupDetail counterDetail)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(_allocator, counterDetail.DocumentId);
                await _actions[shardNumber].WriteCounterAsync(counterDetail);
            }

            public async ValueTask WriteLegacyCounterAsync(CounterDetail counterDetail)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(counterDetail.DocumentId);
                await _actions[shardNumber].WriteLegacyCounterAsync(counterDetail);
            }

            public void RegisterForDisposal(IDisposable data)
            {
            }
        }

        private class ShardedTimeSeriesActions : ShardedActions<ITimeSeriesActions>, ITimeSeriesActions
        {
            public ShardedTimeSeriesActions(ShardedDatabaseContext databaseContext, ITimeSeriesActions[] actions, DatabaseSmugglerOptionsServerSide options) : base(databaseContext, actions, options)
            {
            }

            public async ValueTask WriteTimeSeriesAsync(TimeSeriesItem ts)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(ts.DocId);
                await _actions[shardNumber].WriteTimeSeriesAsync(ts);
            }
        }

        private class ShardedLegacyActions : ShardedActions<ILegacyActions>, ILegacyActions
        {
            public ShardedLegacyActions(ShardedDatabaseContext databaseContext, ILegacyActions[] actions, DatabaseSmugglerOptionsServerSide options) : base(databaseContext, actions, options)
            {
            }

            public async ValueTask WriteLegacyDeletions(string id)
            {
                var shardNumber = DatabaseContext.GetShardNumberFor(id);
                await _actions[shardNumber].WriteLegacyDeletions(id);
            }
        }
    }
}
