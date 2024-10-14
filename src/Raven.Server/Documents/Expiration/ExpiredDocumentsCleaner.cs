//-----------------------------------------------------------------------
// <copyright file="ExpiredDocumentsCleaner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Sparrow.Platform;
using Voron;

namespace Raven.Server.Documents.Expiration
{
    public sealed class ExpiredDocumentsCleaner : BackgroundWorkBase
    {
        public const int DefaultDeleteFrequencyInSec = 60;

        public const int DefaultRefreshFrequencyInSec = 60;

        internal static int BatchSize = PlatformDetails.Is32Bits == false
            ? 4096
            : 1024;

        internal static int DefaultMaxItemsToProcessInSingleRun = int.MaxValue;

        private readonly DocumentDatabase _database;
        private readonly TimeSpan _refreshPeriod;
        private readonly TimeSpan _expirationPeriod;

        public ExpirationConfiguration ExpirationConfiguration { get; }
        public RefreshConfiguration RefreshConfiguration { get; }

        private ExpiredDocumentsCleaner(DocumentDatabase database, ExpirationConfiguration expirationConfiguration, RefreshConfiguration refreshConfiguration) : base(database.Name, database.DatabaseShutdown)
        {
            ExpirationConfiguration = expirationConfiguration;
            RefreshConfiguration = refreshConfiguration;
            _database = database;
            _expirationPeriod = TimeSpan.FromSeconds(ExpirationConfiguration?.DeleteFrequencyInSec ?? DefaultDeleteFrequencyInSec);
            _refreshPeriod = TimeSpan.FromSeconds(RefreshConfiguration?.RefreshFrequencyInSec ?? DefaultRefreshFrequencyInSec);
        }

        public static ExpiredDocumentsCleaner LoadConfigurations(DocumentDatabase database, DatabaseRecord dbRecord, ExpiredDocumentsCleaner expiredDocumentsCleaner)
        {
            try
            {
                if (dbRecord.Expiration == null && dbRecord.Refresh == null)
                {
                    expiredDocumentsCleaner?.Dispose();
                    return null;
                }

                if (expiredDocumentsCleaner != null)
                {
                    // no changes
                    if (Equals(expiredDocumentsCleaner.ExpirationConfiguration, dbRecord.Expiration) &&
                        Equals(expiredDocumentsCleaner.RefreshConfiguration, dbRecord.Refresh))
                        return expiredDocumentsCleaner;
                }

                expiredDocumentsCleaner?.Dispose();

                var hasExpiration = dbRecord.Expiration?.Disabled == false;
                var hasRefresh = dbRecord.Refresh?.Disabled == false;

                if (hasExpiration == false && hasRefresh == false)
                    return null;

                var cleaner = new ExpiredDocumentsCleaner(database, dbRecord.Expiration, dbRecord.Refresh);
                cleaner.Start();
                return cleaner;
            }
            catch (Exception e)
            {
                const string msg = "Cannot enable expired documents cleaner as the configuration record is not valid.";
                database.NotificationCenter.Add(AlertRaised.Create(
                    database.Name,
                    $"Expiration error in {database.Name}", msg,
                    AlertType.RevisionsConfigurationNotValid, NotificationSeverity.Error, database.Name));

                var logger = LoggingSource.Instance.GetLogger<ExpiredDocumentsCleaner>(database.Name);
                if (logger.IsOperationsEnabled)
                    logger.Operations(msg, e);

                return null;
            }
        }

        protected override Task DoWork()
        {
            var expiration = DoExpirationWork();
            var refresh = DoRefreshWork();

            return Task.WhenAll(expiration, refresh);
        }

        private async Task DoRefreshWork()
        {
            while (RefreshConfiguration?.Disabled == false)
            {
                await WaitOrThrowOperationCanceled(_refreshPeriod);

                await RefreshDocs();
            }
        }

        private async Task DoExpirationWork()
        {
            while (ExpirationConfiguration?.Disabled == false)
            {
                await WaitOrThrowOperationCanceled(_expirationPeriod);

                await CleanupExpiredDocs();
            }
        }

        internal Task CleanupExpiredDocs(int? batchSize = null, bool throwOnError = false)
        {
            return CleanupDocs(batchSize ?? BatchSize, ExpirationConfiguration.MaxItemsToProcess ?? DefaultMaxItemsToProcessInSingleRun, forExpiration: true, throwOnError: throwOnError);
        }

        internal Task RefreshDocs(int? batchSize = null, bool throwOnError = false)
        {
            return CleanupDocs(batchSize ?? BatchSize, RefreshConfiguration.MaxItemsToProcess ?? DefaultMaxItemsToProcessInSingleRun, forExpiration: false, throwOnError: throwOnError);
        }
        
        private async Task CleanupDocs(int batchSize, long maxItemsToProcess, bool forExpiration, bool throwOnError)
        {
            var currentTime = _database.Time.GetUtcNow();
            
            try
            {
                DatabaseTopology topology;
                string nodeTag;
                using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    topology = _database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, _database.Name);
                    nodeTag = _database.ServerStore.NodeTag;
                }

                var totalCount = 0;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    while (totalCount < maxItemsToProcess)
                    {
                        context.Reset();
                        context.Renew();

                        Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo> expired;
                        Stopwatch duration;

                        using (context.OpenReadTransaction())
                        {
                            var options = new BackgroundWorkParameters(context, currentTime, topology, nodeTag, batchSize, maxItemsToProcess);

                            expired =
                                forExpiration
                                    ? _database.DocumentsStorage.ExpirationStorage.GetDocuments(options, ref totalCount, out duration, CancellationToken)
                                    : _database.DocumentsStorage.RefreshStorage.GetDocuments(options, ref totalCount, out duration, CancellationToken);

                            if (expired == null || expired.Count == 0)
                                return;
                        }

                        while (expired.Count > 0)
                        {
                            _database.DatabaseShutdown.ThrowIfCancellationRequested();

                            var command = new DeleteExpiredDocumentsCommand(expired, _database, forExpiration, currentTime);
                            await _database.TxMerger.Enqueue(command);

                            if (Logger.IsInfoEnabled)
                                Logger.Info($"Successfully {(forExpiration ? "deleted" : "refreshed")} {command.DeletionCount:#,#;;0} documents in {duration.ElapsedMilliseconds:#,#;;0} ms.");
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // this will stop processing
                throw;
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to {(forExpiration ? "delete" : "refresh")} documents on {_database.Name} which are older than {currentTime}", e);

                if (throwOnError)
                    throw;
            }
        }

        internal sealed class DeleteExpiredDocumentsCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo> _expired;
            private readonly DocumentDatabase _database;
            private readonly bool _forExpiration;
            private readonly DateTime _currentTime;

            public int DeletionCount;

            public DeleteExpiredDocumentsCommand(Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo> expired, DocumentDatabase database, bool forExpiration, DateTime currentTime)
            {
                _expired = expired;
                _database = database;
                _forExpiration = forExpiration;
                _currentTime = currentTime;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                DeletionCount =
                    _forExpiration
                        ? _database.DocumentsStorage.ExpirationStorage.ProcessDocuments(context, _expired, _currentTime)
                        : _database.DocumentsStorage.RefreshStorage.ProcessDocuments(context, _expired, _currentTime);

                return DeletionCount;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
            {
                return new DeleteExpiredDocumentsCommandDto
                {
                    Expired = _expired.Select(x => (Ticks: x.Ticks, LowerId: x.LowerId, Id: x.Id)).ToArray(),
                    ForExpiration = _forExpiration,
                    CurrentTime = _currentTime
                };
            }
        }
    }

    internal sealed class DeleteExpiredDocumentsCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, ExpiredDocumentsCleaner.DeleteExpiredDocumentsCommand>
    {
        public ExpiredDocumentsCleaner.DeleteExpiredDocumentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var expired = new Queue<AbstractBackgroundWorkStorage.DocumentExpirationInfo>();
            foreach (var item in Expired)
            {
                expired.Enqueue(new AbstractBackgroundWorkStorage.DocumentExpirationInfo(item.Item1.Clone(context.Allocator), item.Item2.Clone(context.Allocator), item.Item3));
            }
            var command = new ExpiredDocumentsCleaner.DeleteExpiredDocumentsCommand(expired, database, ForExpiration, CurrentTime);
            return command;
        }

        public (Slice, Slice, string)[] Expired { get; set; }

        public bool ForExpiration { get; set; }

        public DateTime CurrentTime { get; set; }
    }
}
