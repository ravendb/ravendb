//-----------------------------------------------------------------------
// <copyright file="ExpiredDocumentsCleaner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Expiration;
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
        private readonly TimeSpan _period;

        public ExpirationConfiguration Configuration { get; }

        private ExpiredDocumentsCleaner(DocumentDatabase database, ExpirationConfiguration configuration) : base(database.Name, database.DatabaseShutdown)
        {
            Configuration = configuration;
            _database = database;

            var deleteFrequencyInSeconds = configuration.DeleteFrequencyInSec ?? 60;
            if (Logger.IsInfoEnabled)
                Logger.Info($"Initialized expired document cleaner, will check for expired documents every {deleteFrequencyInSeconds} seconds");

            _period = TimeSpan.FromSeconds(deleteFrequencyInSeconds);
        }

        public static ExpiredDocumentsCleaner LoadConfigurations(DocumentDatabase database, DatabaseRecord dbRecord, ExpiredDocumentsCleaner expiredDocumentsCleaner)
        {
            try
            {
                if (dbRecord.Expiration == null)
                {
                    expiredDocumentsCleaner?.Dispose();
                    return null;
                }
                if (dbRecord.Expiration.Equals(expiredDocumentsCleaner?.Configuration))
                    return expiredDocumentsCleaner;
                expiredDocumentsCleaner?.Dispose();
                if (dbRecord.Expiration.Disabled)
                    return null;

                var cleaner = new ExpiredDocumentsCleaner(database, dbRecord.Expiration);
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

        protected override async Task DoWork()
        {
            await WaitOrThrowOperationCanceled(_period);

            await CleanupExpiredDocs();
        }

        internal async Task CleanupExpiredDocs()
        {
            var currentTime = _database.Time.GetUtcNow();

            try
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Trying to find expired documents to delete");

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    using (context.OpenReadTransaction())
                    {
                        var expired = _database.DocumentsStorage.ExpirationStorage.GetExpiredDocuments(context, currentTime, out var duration, CancellationToken);
                        if (expired == null || expired.Count == 0)
                            return;

                        var command = new DeleteExpiredDocumentsCommand(expired, _database);
                        await _database.TxMerger.Enqueue(command);
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Successfully deleted {command.DeletionCount:#,#;;0} documents in {duration.ElapsedMilliseconds:#,#;;0} ms.");
                    }
                }
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to delete expired documents on {_database.Name} which are older than {currentTime}", e);
            }
        }

        internal class DeleteExpiredDocumentsCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly Dictionary<Slice, List<(Slice LowerId, LazyStringValue Id)>> _expired;
            private readonly DocumentDatabase _database;

            public int DeletionCount;

            public DeleteExpiredDocumentsCommand(Dictionary<Slice, List<(Slice LowerId, LazyStringValue Id)>> expired, DocumentDatabase database)
            {
                _expired = expired;
                _database = database;
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                return DeletionCount = _database.DocumentsStorage.ExpirationStorage.DeleteExpiredDocuments(context, _expired);
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

                return new DeleteExpiredDocumentsCommandDto { Expired = keyValuePairs };
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
            var command = new ExpiredDocumentsCleaner.DeleteExpiredDocumentsCommand(expired, database);
            return command;
        }

        public KeyValuePair<Slice, List<(Slice LowerId, LazyStringValue Id)>>[] Expired { get; set; }
    }
}
