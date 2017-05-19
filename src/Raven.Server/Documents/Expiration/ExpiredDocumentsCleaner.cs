//-----------------------------------------------------------------------
// <copyright file="ExpiredDocumentsCleaner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Expiration;
using Raven.Server.Json;
using Raven.Server.Background;
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

        private const string DocumentsByExpiration = "DocumentsByExpiration";

        public ExpirationConfiguration Configuration { get; }

        private ExpiredDocumentsCleaner(DocumentDatabase database, ExpirationConfiguration configuration) : base(database.Name, database.DatabaseShutdown)
        {
            Configuration = configuration;
            _database = database;

            var deleteFrequencyInSeconds = configuration.DeleteFrequencySeconds ?? 60;
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
                if (dbRecord.Expiration.Active == false)
                    return null;

                var cleaner = new ExpiredDocumentsCleaner(database, dbRecord.Expiration);
                cleaner.Start();
                return cleaner;
            }
            catch (Exception e)
            {
                //TODO: Raise alert, or maybe handle this via a db load error that can be turned off with 
                //TODO: a config

                var logger = LoggingSource.Instance.GetLogger<ExpiredDocumentsCleaner>(database.Name);

                if (logger.IsOperationsEnabled)
                    logger.Operations("Cannot enable expired documents cleaner as the configuration record is not valid.", e);

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
            var currentTicks = currentTime.Ticks;

            try
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Trying to find expired documents to delete");
                
                DocumentsOperationContext context;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    using (var tx = context.OpenReadTransaction())
                    {
                        var expirationTree = tx.InnerTransaction.CreateTree(DocumentsByExpiration);

                        Dictionary<Slice, List<(Slice LoweredKey, LazyStringValue Key)>> expired;
                        Stopwatch duration;

                        using (var it = expirationTree.Iterate(false))
                        {
                            if (it.Seek(Slices.BeforeAllKeys) == false)
                                return;

                            expired = new Dictionary<Slice, List<(Slice LoweredKey, LazyStringValue Key)>>();
                            duration = Stopwatch.StartNew();

                            do
                            {
                                var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                                if (entryTicks >= currentTicks)
                                    return;

                                var ticksAsSlice = it.CurrentKey.Clone(tx.InnerTransaction.Allocator);

                                var expiredDocs = new List<(Slice LoweredKey, LazyStringValue Key)>();

                                expired.Add(ticksAsSlice, expiredDocs);

                                using (var multiIt = expirationTree.MultiRead(it.CurrentKey))
                                {
                                    if (multiIt.Seek(Slices.BeforeAllKeys))
                                    {
                                        do
                                        {
                                            if (CancellationToken.IsCancellationRequested)
                                                return;

                                            var clonedKey = multiIt.CurrentKey.Clone(tx.InnerTransaction.Allocator);

                                            var document = _database.DocumentsStorage.Get(context, clonedKey);
                                            if (document == null)
                                            {
                                                expiredDocs.Add((clonedKey, null));
                                                continue;
                                            }

                                            // Validate that the expiration value in metadata is still the same.
                                            // We have to check this as the user can update this valud.
                                            string expirationDate;
                                            BlittableJsonReaderObject metadata;
                                            if (document.Data.TryGet(Constants.Documents.Metadata.Key, out metadata) == false ||
                                                metadata.TryGet(Constants.Documents.Expiration.ExpirationDate, out expirationDate) == false)
                                                continue;

                                            DateTime date;
                                            if (DateTime.TryParseExact(expirationDate, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind,
                                                    out date) == false)
                                                continue;

                                            if (currentTime < date)
                                                continue;

                                            expiredDocs.Add((clonedKey, document.Key));

                                        } while (multiIt.MoveNext());
                                    }
                                }

                            } while (it.MoveNext());
                        }

                        var command = new DeleteExpiredDocumentsCommand(expired, _database, Logger);

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

        public unsafe void Put(DocumentsOperationContext context,
            Slice loweredKey, BlittableJsonReaderObject document)
        {
            string expirationDate;
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Documents.Metadata.Key, out metadata) == false ||
                metadata.TryGet(Constants.Documents.Expiration.ExpirationDate, out expirationDate) == false)
                return;

            DateTime date;
            if (DateTime.TryParseExact(expirationDate, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out date) == false)
                throw new InvalidOperationException($"The expiration date format is not valid: '{expirationDate}'. Use the following format: {_database.Time.GetUtcNow().ToString("O")}");

            // We explicitly enable adding documents that have already been expired, we have to, because if the time lag is short, it is possible
            // that we add a document that expire in 1 second, but by the time we process it, it already expired. The user did nothing wrong here
            // and we'll use the normal cleanup routine to clean things up later.

            var ticksBigEndian = IPAddress.HostToNetworkOrder(date.Ticks);

            var tree = context.Transaction.InnerTransaction.CreateTree(DocumentsByExpiration);
            Slice ticksSlice;
            using (Slice.External(context.Allocator, (byte*) &ticksBigEndian, sizeof(long), out ticksSlice))
                tree.MultiAdd(ticksSlice, loweredKey);
        }

        private class DeleteExpiredDocumentsCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly Dictionary<Slice, List<(Slice LoweredKey, LazyStringValue Key)>> _expired;
            private readonly DocumentDatabase _database;
            private readonly Logger _logger;

            public int DeletionCount;

            public DeleteExpiredDocumentsCommand(Dictionary<Slice, List<(Slice LoweredKey, LazyStringValue Key)>> expired, DocumentDatabase database, Logger logger)
            {
                _expired = expired;
                _database = database;
                _logger = logger;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                var expirationTree = context.Transaction.InnerTransaction.CreateTree(DocumentsByExpiration);

                foreach (var expired in _expired)
                {
                    foreach (var ids in expired.Value)
                    {
                        if (ids.Key != null)
                        {
                            var deleted = _database.DocumentsStorage.Delete(context, ids.LoweredKey, ids.Key, expectedEtag: null);

                            if (_logger.IsInfoEnabled && deleted == null)
                                _logger.Info($"Tried to delete expired document '{ids.Key}' but document was not found.");

                            DeletionCount++;
                        }

                        expirationTree.MultiDelete(expired.Key, ids.LoweredKey);
                    }
                }

                return DeletionCount;
            }
        }
    }
}