using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow;
using Sparrow.Json;
using Sparrow.Platform;
using Size = Sparrow.Size;

namespace Raven.Server.Smuggler.Documents.Actions
{
    internal abstract class AbstractDatabaseCompareExchangeActions : ICompareExchangeActions
    {
        private const int BatchSize = 10 * 1024;

        private readonly Size _compareExchangeValuesBatchSize;
        private Size _compareExchangeValuesSize;

        private readonly Size _clusterTransactionCommandsBatchSize;
        protected Size _clusterTransactionCommandsSize;

        private readonly JsonOperationContext _context;

        protected readonly ServerStore _serverStore;
        protected readonly string _databaseName;
        private readonly char _identityPartsSeparator;

        private readonly List<RemoveCompareExchangeCommand> _compareExchangeRemoveCommands = new List<RemoveCompareExchangeCommand>();
        private readonly List<AddOrUpdateCompareExchangeCommand> _compareExchangeAddOrUpdateCommands = new List<AddOrUpdateCompareExchangeCommand>();
        protected DisposableReturnedArray<ClusterTransactionCommand.ClusterTransactionDataCommand> _clusterTransactionCommands = new DisposableReturnedArray<ClusterTransactionCommand.ClusterTransactionDataCommand>(BatchSize);

        protected long? _lastAddOrUpdateOrRemoveResultIndex;
        protected long? _lastClusterTransactionIndex;
        protected readonly BackupKind? _backupKind;
        protected readonly CancellationToken _token;

        protected AbstractDatabaseCompareExchangeActions(ServerStore serverStore, string databaseName, char identityPartsSeparator, JsonOperationContext context, BackupKind? backupKind, CancellationToken token)
        {
            _serverStore = serverStore;
            _databaseName = databaseName;
            _identityPartsSeparator = identityPartsSeparator;
            _context = context;
            _backupKind = backupKind;
            _token = token;

            var is32Bits = PlatformDetails.Is32Bits || serverStore.Configuration.Storage.ForceUsing32BitsPager;

            _compareExchangeValuesBatchSize = new Size(is32Bits ? 2 : 4, SizeUnit.Megabytes);
            _compareExchangeValuesSize = new Size(0, SizeUnit.Megabytes);

            _clusterTransactionCommandsBatchSize = new Size(is32Bits ? 2 : 16, SizeUnit.Megabytes);
            _clusterTransactionCommandsSize = new Size(0, SizeUnit.Megabytes);
        }

        protected abstract bool TryHandleAtomicGuard(string key, string documentId, BlittableJsonReaderObject value, Document existingDocument);

        public async ValueTask<bool> WriteKeyValueAsync(string key, BlittableJsonReaderObject value, Document existingDocument)
        {
            var anyCommandsSent = false;
            if (_compareExchangeValuesSize >= _compareExchangeValuesBatchSize || _compareExchangeAddOrUpdateCommands.Count >= BatchSize)
            {
                anyCommandsSent |= await SendAddOrUpdateCommandsAsync();
                _compareExchangeValuesSize.Set(0, SizeUnit.Bytes);
            }

            if (_clusterTransactionCommandsSize >= _clusterTransactionCommandsBatchSize || _clusterTransactionCommands.Length >= BatchSize)
            {
                anyCommandsSent |= await SendClusterTransactionsAsync();
                _clusterTransactionCommandsSize.Set(0, SizeUnit.Bytes);
            }

            if (ClusterWideTransactionHelper.IsAtomicGuardKey(key, out var docId) && TryHandleAtomicGuard(key, docId, value, existingDocument))
                return anyCommandsSent;

            _compareExchangeAddOrUpdateCommands.Add(new AddOrUpdateCompareExchangeCommand(_databaseName, key, value, 0, _context, RaftIdGenerator.DontCareId, fromBackup: true));
            _compareExchangeValuesSize.Add(value.Size, SizeUnit.Bytes);

            return anyCommandsSent;
        }

        public async ValueTask WriteTombstoneKeyAsync(string key)
        {
            var index = _serverStore.LastRaftCommitIndex;
            _compareExchangeRemoveCommands.Add(new RemoveCompareExchangeCommand(_databaseName, key, index, _context, RaftIdGenerator.DontCareId, fromBackup: true));

            if (_compareExchangeRemoveCommands.Count < BatchSize)
                return;

            await SendRemoveCommandsAsync();
        }

        public async ValueTask FlushAsync()
        {
            await SendClusterTransactionsAsync();
        }

        protected abstract ValueTask WaitForIndexNotificationAsync(long? lastAddOrUpdateOrRemoveResultIndex, long? lastClusterTransactionIndex);

        public virtual async ValueTask DisposeAsync()
        {
            using (_clusterTransactionCommands)
            {
                await SendClusterTransactionsAsync();
                await SendAddOrUpdateCommandsAsync();
                await SendRemoveCommandsAsync();

                await WaitForIndexNotificationAsync(_lastAddOrUpdateOrRemoveResultIndex, _lastClusterTransactionIndex);
            }
        }

        protected virtual ClusterTransactionCommand CreateClusterTransactionCommand(string databaseName, char identityPartsSeparator, ArraySegment<ClusterTransactionCommand.ClusterTransactionDataCommand> parsedCommands, ClusterTransactionCommand.ClusterTransactionOptions options, string raftRequestId)
        {
            var topology = _serverStore.LoadDatabaseTopology(_databaseName);

            return new ClusterTransactionCommand(_databaseName, _identityPartsSeparator, topology, parsedCommands, options, raftRequestId);
        }

        protected virtual ValueTask<bool> SendClusterTransactionsAsync()
        {
            if (_clusterTransactionCommands.Length == 0)
                return ValueTask.FromResult(false);

            return new ValueTask<bool>(AsyncWork());

            async Task<bool> AsyncWork()
            {
                var parsedCommands = _clusterTransactionCommands.GetArraySegment();

                var raftRequestId = RaftIdGenerator.NewId();
                var options = new ClusterTransactionCommand.ClusterTransactionOptions(taskId: raftRequestId, disableAtomicDocumentWrites: false,
                    _serverStore.Engine.CommandsVersionManager.CurrentClusterMinimalVersion);

                var clusterTransactionCommand = CreateClusterTransactionCommand(_databaseName, _identityPartsSeparator, parsedCommands, options, raftRequestId);
                clusterTransactionCommand.FromBackup = true;

                var clusterTransactionResult = await _serverStore.SendToLeaderAsync(clusterTransactionCommand);
                for (int i = 0; i < _clusterTransactionCommands.Length; i++)
                {
                    _clusterTransactionCommands[i].Document.Dispose();
                }

                _clusterTransactionCommands.Clear();

                if (clusterTransactionResult.Result is List<ClusterTransactionCommand.ClusterTransactionErrorInfo> errors)
                    throw new ClusterTransactionConcurrencyException(
                        $"Failed to execute cluster transaction due to the following issues: {string.Join(Environment.NewLine, errors.Select(e => e.Message))}")
                    {
                        ConcurrencyViolations = errors.Select(e => e.Violation).ToArray()
                    };

                _lastClusterTransactionIndex = clusterTransactionResult.Index;

                return true;
            }
        }

        private async ValueTask<bool> SendAddOrUpdateCommandsAsync()
        {
            if (_compareExchangeAddOrUpdateCommands.Count == 0)
                return false;

            var addOrUpdateResult = await _serverStore.SendToLeaderAsync(new AddOrUpdateCompareExchangeBatchCommand(_compareExchangeAddOrUpdateCommands, RaftIdGenerator.DontCareId));
            foreach (var command in _compareExchangeAddOrUpdateCommands)
            {
                command.Value.Dispose();
            }
            _compareExchangeAddOrUpdateCommands.Clear();

            _lastAddOrUpdateOrRemoveResultIndex = addOrUpdateResult.Index;

            return true;
        }

        private async ValueTask SendRemoveCommandsAsync()
        {
            if (_compareExchangeRemoveCommands.Count == 0)
                return;
            var addOrUpdateResult = await _serverStore.SendToLeaderAsync(new AddOrUpdateCompareExchangeBatchCommand(_compareExchangeRemoveCommands, RaftIdGenerator.DontCareId));
            _compareExchangeRemoveCommands.Clear();

            _lastAddOrUpdateOrRemoveResultIndex = addOrUpdateResult.Index;
        }

        protected struct DisposableReturnedArray<T> : IDisposable
        {
            private readonly T[] _array;

            private readonly int _maxLength;

            public int Length;

            public DisposableReturnedArray(int length)
            {
                _array = ArrayPool<T>.Shared.Rent(length);
                _maxLength = length;
                Length = 0;
            }

            public void Push(T item)
            {
                if (Length >= _maxLength)
                    throw new InvalidOperationException($"Cannot put more than {_maxLength} elements to the array.");

                _array[Length] = item;
                Length++;
            }

            public T this[int index] => _array[index];

            public ArraySegment<T> GetArraySegment() => new ArraySegment<T>(_array, 0, Length);

            public void Clear() => Length = 0;

            public void Dispose() => ArrayPool<T>.Shared.Return(_array);
        }
    }
}
