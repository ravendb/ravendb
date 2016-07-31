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
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Sparrow.Logging;

namespace Raven.Server.Documents.Expiration
{
    public class ExpiredDocumentsCleaner : IDisposable
    {
        private readonly DocumentDatabase _database;

        public Func<DateTime> UtcNow = () => DateTime.UtcNow;

        private static Logger _logger;
        private const string DocumentsByExpiration = "DocumentsByExpiration";

        private readonly Timer _timer;
        private readonly object _locker = new object();

        private ExpiredDocumentsCleaner(DocumentDatabase database, ExpirationConfiguration configuration)
        {
            _database = database;
            _logger = LoggerSetup.Instance.GetLogger<ExpiredDocumentsCleaner>(database.Name);
            var deleteFrequencyInSeconds = configuration.DeleteFrequencySeconds ?? 60;
            if (_logger.IsInfoEnabled)
                _logger.Info($"Initialized expired document cleaner, will check for expired documents every {deleteFrequencyInSeconds} seconds");

            var period = TimeSpan.FromSeconds(deleteFrequencyInSeconds);
            _timer = new Timer(TimerCallback, null, period, period);
        }

        public static ExpiredDocumentsCleaner LoadConfigurations(DocumentDatabase database)
        {
            DocumentsOperationContext context;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var configuration = database.DocumentsStorage.Get(context, Constants.Expiration.ConfigurationDocumentKey);
                if (configuration == null)
                    return null;

                try
                {
                    var expirationConfiguration = JsonDeserializationServer.ExpirationConfiguration(configuration.Data);
                    if (expirationConfiguration.Active == false)
                        return null;

                    return new ExpiredDocumentsCleaner(database, expirationConfiguration);
                }
                catch (Exception e)
                {
                    //TODO: Raise alert, or maybe handle this via a db load error that can be turned off with 
                    //TODO: a config
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Cannot enable expired documents cleaner as the configuration document {Constants.Expiration.ConfigurationDocumentKey} is not valid: {configuration.Data}", e);
                    return null;
                }
            }
        }

        public void TimerCallback(object _)
        {
            if (_database.DatabaseShutdown.IsCancellationRequested)
                return;

            if (Monitor.TryEnter(_locker) == false)
                return;

            try
            {
                CleanupExpiredDocs();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error when trying to find expired documents", e);
            }
            finally
            {
                Monitor.Exit(_locker);
            }
        }

        public void CleanupExpiredDocs()
        {
            if (_logger.IsInfoEnabled)
                _logger.Info("Trying to find expired documents to delete");

            bool exitWriteTransactionAndContinueAgain = true;
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var currentTime = UtcNow();

                while (exitWriteTransactionAndContinueAgain)
                {
                    exitWriteTransactionAndContinueAgain = CleanupDocumentsOnce(context, currentTime);

                    if (exitWriteTransactionAndContinueAgain)
                        Thread.Sleep(16); // give up the thread for a short while, to let other transactions run
                }
            }
        }

        private bool CleanupDocumentsOnce(DocumentsOperationContext context, DateTime currentTime)
        {
            int count = 0;
            var earlyExit = false;
            var keysToDelete = new List<Slice>();
            var currentTicks = currentTime.Ticks;
            var sp = Stopwatch.StartNew();

            using (var tx = context.OpenWriteTransaction())
            {
                var expirationTree = tx.InnerTransaction.CreateTree(DocumentsByExpiration);
                while (true)
                {
                    using (var it = expirationTree.Iterate(false))
                    {
                        if (it.Seek(Slices.BeforeAllKeys) == false)
                            break;
                        var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                        if (entryTicks >= currentTicks)
                            break;
                        using (var multiIt = expirationTree.MultiRead(it.CurrentKey))
                        {
                            if (multiIt.Seek(Slices.BeforeAllKeys))
                            {
                                do
                                {
                                    if (sp.ElapsedMilliseconds > 150)
                                    {
                                        earlyExit = true;
                                        break;
                                    }

                                    var clonedKey = multiIt.CurrentKey.Clone(tx.InnerTransaction.Allocator);
                                    keysToDelete.Add(clonedKey);

                                    var document = _database.DocumentsStorage.Get(context, clonedKey);
                                    if (document == null)
                                        continue;

                                    // Validate that the expiration value in metadata is still the same.
                                    // We have to check this as the user can update this valud.
                                    string expirationDate;
                                    BlittableJsonReaderObject metadata;
                                    if (document.Data.TryGet(Constants.Metadata, out metadata) == false ||
                                        metadata.TryGet(Constants.Expiration.RavenExpirationDate, out expirationDate) == false)
                                        continue;

                                    DateTime date;
                                    if (DateTime.TryParseExact(expirationDate, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out date) == false)
                                        continue;
                                    if (currentTime < date)
                                        continue;

                                    var deleted = _database.DocumentsStorage.Delete(context, clonedKey, null);
                                    count++;
                                    if (_logger.IsInfoEnabled && deleted == false)
                                        _logger.Info($"Tried to delete expired document '{clonedKey}' but document was not found.");
                                } while (multiIt.MoveNext());
                            }
                        }
                        var treeKey = it.CurrentKey.Clone(tx.InnerTransaction.Allocator);
                        foreach (var slice in keysToDelete)
                        {
                            expirationTree.MultiDelete(treeKey, slice);
                        }
                    }
                    if (earlyExit)
                        break;
                }

                tx.Commit();
            }
            if (_logger.IsInfoEnabled)
                _logger.Info($"Successfully deleted {count:#,#;;0} documents in {sp.ElapsedMilliseconds:#,#;;0} ms. Found more stuff to delete? {earlyExit}");
            return earlyExit;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            _timer.Dispose();
        }

        public unsafe void Put(DocumentsOperationContext context,
            Slice loweredKey, BlittableJsonReaderObject document)
        {
            string expirationDate;
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Metadata, out metadata) == false ||
                metadata.TryGet(Constants.Expiration.RavenExpirationDate, out expirationDate) == false)
                return;

            DateTime date;
            if (DateTime.TryParseExact(expirationDate, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out date) == false)
                throw new InvalidOperationException($"The expiration date format is not valid: '{expirationDate}'. Use the following format: {UtcNow().ToString("O")}");

            // We explicitly enable adding documents that have already been expired, we have to, because if the time lag is short, it is possible
            // that we add a document that expire in 1 second, but by the time we process it, it already expired. The user did nothing wrong here
            // and we'll use the normal cleanup routine to clean things up later.

            var ticksBigEndian = IPAddress.HostToNetworkOrder(date.Ticks);

            var tree = context.Transaction.InnerTransaction.CreateTree(DocumentsByExpiration);
            tree.MultiAdd(Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long)), loweredKey);
        }
    }
}