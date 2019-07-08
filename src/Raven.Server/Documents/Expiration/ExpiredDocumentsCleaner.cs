//-----------------------------------------------------------------------
// <copyright file="ExpiredDocumentsCleaner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Expiration
{
    public class ExpiredDocumentsCleaner : BackgroundWorkBase
    {
        private readonly DocumentDatabase _database;

        public ExpirationConfiguration ExpirationConfiguration { get; }
        public RefreshConfiguration RefreshConfiguration { get; }

        private ExpiredDocumentsCleaner(DocumentDatabase database, ExpirationConfiguration expirationConfiguration, RefreshConfiguration refreshConfiguration) : base(database.Name, database.DatabaseShutdown)
        {
            ExpirationConfiguration = expirationConfiguration;
            RefreshConfiguration = refreshConfiguration;
            _database = database;
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
                    if (object.Equals(expiredDocumentsCleaner.ExpirationConfiguration, dbRecord.Expiration) &&
                        object.Equals(expiredDocumentsCleaner.RefreshConfiguration, dbRecord.Refresh))
                        return expiredDocumentsCleaner;
                }

                expiredDocumentsCleaner?.Dispose();
                if (dbRecord.Expiration?.Disabled == true && dbRecord.Refresh?.Disabled == true)
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

        private DateTime _nextRefresh, _nextExpiration;

        protected override Task DoWork()
        {
            var expr = DoExpirationWork();
            var refresh = DoRefreshWork();

            return Task.WhenAll(expr, refresh);
        }

        private async Task DoRefreshWork()
        {
            while (RefreshConfiguration?.Disabled == false)
            {
                await WaitOrThrowOperationCanceled(_nextRefresh - DateTime.UtcNow);
                _nextRefresh = DateTime.UtcNow.AddSeconds(RefreshConfiguration?.RefreshFrequencyInSec ?? 60);
                await RefreshDocs();
            }
        }

        private async Task DoExpirationWork()
        {
            while (ExpirationConfiguration?.Disabled == false)
            {
                await WaitOrThrowOperationCanceled(_nextExpiration - DateTime.UtcNow);
                _nextExpiration = DateTime.UtcNow.AddSeconds(ExpirationConfiguration?.DeleteFrequencyInSec ?? 60);
                await CleanupExpiredDocs();
            }
        }

        internal Task CleanupExpiredDocs()
        {
            return CleanupDocs(forExpiration: true);
        }

        internal Task RefreshDocs()
        {
            return CleanupDocs(forExpiration: false);
        }

        private async Task CleanupDocs(bool forExpiration)
        {
            var currentTime = _database.Time.GetUtcNow();

            try
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Trying to find {(forExpiration ? "expired" : "require refreshing")} documents to delete");

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    DatabaseTopology topology;
                    using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                    using (serverContext.OpenReadTransaction())
                    {
                        topology = _database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, _database.Name);
                    }

                    var isFirstInTopology = string.Equals(topology.AllNodes.FirstOrDefault(), _database.ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase);

                    using (context.OpenReadTransaction())
                    {
                        var expired =
                            forExpiration ?
                                _database.DocumentsStorage.ExpirationStorage.GetExpiredDocuments(context, currentTime, isFirstInTopology, out var duration, CancellationToken) :
                                _database.DocumentsStorage.ExpirationStorage.GetDocumentsToRefresh(context, currentTime, isFirstInTopology, out duration, CancellationToken);
                        if (expired == null || expired.Count == 0)
                            return;

                        var command = new DeleteExpiredDocumentsCommand(expired, _database, forExpiration);
                        await _database.TxMerger.Enqueue(command);
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Successfully {(forExpiration ? "deleted" : "refreshed")} {command.DeletionCount:#,#;;0} documents in {duration.ElapsedMilliseconds:#,#;;0} ms.");
                    }
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to expire/refresh documents on {_database.Name} which are older than {currentTime}", e);
            }
        }

        internal class DeleteExpiredDocumentsCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly Dictionary<Slice, List<(Slice LowerId, LazyStringValue Id)>> _expired;
            private readonly DocumentDatabase _database;
            private readonly bool _forExpiration;

            public int DeletionCount;

            public DeleteExpiredDocumentsCommand(Dictionary<Slice, List<(Slice LowerId, LazyStringValue Id)>> expired, DocumentDatabase database, bool forExpiration)
            {
                _expired = expired;
                _database = database;
                _forExpiration = forExpiration;
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                DeletionCount =
                    _forExpiration
                        ? _database.DocumentsStorage.ExpirationStorage.DeleteDocumentsExpiration(context, _expired)
                        : _database.DocumentsStorage.ExpirationStorage.RefreshDocuments(context, _expired);

                return DeletionCount;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {

                var keyValuePairs = new KeyValuePair<Slice, List<(Slice LowerId, LazyStringValue Id)>>[_expired.Count];
                var i = 0;
                foreach (var item in _expired)
                {
                    keyValuePairs[i] = item;
                    i++;
                }

                return new DeleteExpiredDocumentsCommandDto
                {
                    ForExpiration = _forExpiration,
                    Expired = keyValuePairs
                };
            }
        }
    }

    internal class DeleteExpiredDocumentsCommandDto : TransactionOperationsMerger.IReplayableCommandDto<ExpiredDocumentsCleaner.DeleteExpiredDocumentsCommand>
    {
        public ExpiredDocumentsCleaner.DeleteExpiredDocumentsCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var expired = new Dictionary<Slice, List<(Slice LowerId, LazyStringValue Id)>>();
            foreach (var item in Expired)
            {
                expired[item.Key] = item.Value;
            }
            var command = new ExpiredDocumentsCleaner.DeleteExpiredDocumentsCommand(expired, database, ForExpiration);
            return command;
        }

        public bool ForExpiration { get; set; }

        public KeyValuePair<Slice, List<(Slice LowerId, LazyStringValue Id)>>[] Expired { get; set; }
    }
}
