using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Actions;

internal class DatabaseCompareExchangeActions : AbstractDatabaseCompareExchangeActions
{
    private readonly DocumentContextHolder _documentContextHolder;

    private readonly DocumentDatabase _database;

    private readonly SmugglerResult _result;
    private readonly Action<IOperationProgress> _onProgress;

    public DatabaseCompareExchangeActions([NotNull] string databaseName, [NotNull] DocumentDatabase database, JsonOperationContext context, BackupKind? backupKind, SmugglerResult result, Action<IOperationProgress> onProgress, CancellationToken token)
        : base(database.ServerStore, databaseName, database.IdentityPartsSeparator, context, backupKind, token)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _documentContextHolder = new DocumentContextHolder(database);
        _result = result;
        _onProgress = onProgress;
    }

    protected override bool TryHandleAtomicGuard(string key, string documentId, BlittableJsonReaderObject value, Document existingDocument)
    {
        value?.Dispose();

        var ctx = _documentContextHolder.GetContextForRead();

        Document doc;
        if (existingDocument != null)
        {
            doc = existingDocument;
            doc.Data = doc.Data.Clone(ctx);
        }
        else
        {
            if (_backupKind is BackupKind.Full or BackupKind.Incremental)
            {
                // if we are restoring from a backup, we'll check if the atomic guard already exists
                // if it does, we don't need to save it again
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var result = _database.CompareExchangeStorage.GetCompareExchangeValue(context, key);
                    if (result.Value != null)
                        return true;
                }
            }

            doc = _database.DocumentsStorage.Get(ctx, documentId, DocumentFields.Data | DocumentFields.ChangeVector | DocumentFields.Id);
            if (doc == null)
                return true;
        }

        _clusterTransactionCommands.Push(new ClusterTransactionCommand.ClusterTransactionDataCommand
        {
            Id = doc.Id,
            Document = doc.Data,
            Type = CommandType.PUT,
            ChangeVector = ctx.GetLazyString(doc.ChangeVector),
            FromBackup = _backupKind
        });

        _clusterTransactionCommandsSize.Add(doc.Data.Size, SizeUnit.Bytes);

        return true;
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long? lastAddOrUpdateOrRemoveResultIndex, long? lastClusterTransactionIndex)
    {
        if (_lastAddOrUpdateOrRemoveResultIndex != null)
            await _database.ServerStore.Cluster.WaitForIndexNotification(_lastAddOrUpdateOrRemoveResultIndex.Value, TimeSpan.FromMinutes(1));

        if (_lastClusterTransactionIndex != null)
        {
            await _database.ServerStore.Cluster.WaitForIndexNotification(_lastClusterTransactionIndex.Value, TimeSpan.FromMinutes(1));

            if (_backupKind is null or BackupKind.None)
            {
                // waiting for the commands to be applied
                await _database.ClusterWideTransactionIndexWaiter.WaitAsync(_lastClusterTransactionIndex.Value, _token);
            }
            else
            {
                Debug.Assert(_backupKind == BackupKind.Full || _backupKind == BackupKind.Incremental);
                ExecuteClusterTransactions();
            }
        }
    }

    private void ExecuteClusterTransactions()
    {
        long totalExecutedCommands;
        using (_database.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext serverContext))
        using (serverContext.OpenReadTransaction())
        {
            var commandsCountPerDatabase = serverContext.Transaction.InnerTransaction.ReadTree(ClusterStateMachine.TransactionCommandsCountPerDatabase);
            var prevCount = ClusterTransactionCommand.GetPrevCount(serverContext, commandsCountPerDatabase, _database.Name);
            totalExecutedCommands = prevCount;
        }

        //when restoring from a backup, the database doesn't exist yet and we cannot rely on the DocumentDatabase to execute the database cluster transaction commands
        while (true)
        {
            _token.ThrowIfCancellationRequested();

            using (_database.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                // the commands are already batched (10k or 16MB), so we are executing only 1 at a time
                var executed = _database.ExecuteClusterTransaction(serverContext, batchSize: 1);
                if (executed.BatchSize == 0)
                    break;

                totalExecutedCommands += executed.CommandsCount;
                if (_result != null)
                {
                    _result.AddInfo($"Executed {totalExecutedCommands:#,#;;0} cluster transaction commands.");
                    _onProgress?.Invoke(_result.Progress);
                }
            }
        }
    }

    protected override async ValueTask<bool> SendClusterTransactionsAsync()
    {
        var send = await base.SendClusterTransactionsAsync();
        if (send)
            _documentContextHolder.Reset();

        return send;
    }

    public override async ValueTask DisposeAsync()
    {
        using (_documentContextHolder)
            await base.DisposeAsync();
    }

    private sealed class DocumentContextHolder : IDisposable
    {
        private readonly DocumentDatabase _database;
        private IDisposable _returnContext;
        private DocumentsTransaction _readTx;
        private DocumentsOperationContext _current;

        public DocumentContextHolder(DocumentDatabase database)
        {
            _database = database;
        }

        public DocumentsOperationContext GetContextForRead()
        {
            if (_current != null)
                return _current;

            _returnContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _current);
            _readTx = _current.OpenReadTransaction();
            return _current;
        }

        public void Reset()
        {
            using (_returnContext)
            using (_readTx)
            {

            }

            _returnContext = null;
            _readTx = null;
            _current = null;
        }

        public void Dispose()
        {
            using (_returnContext)
            using (_readTx)
            {
            }
        }
    }
}
