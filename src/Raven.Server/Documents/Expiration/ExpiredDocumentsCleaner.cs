//-----------------------------------------------------------------------
// <copyright file="ExpiredDocumentsCleaner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Expiration
{
    public class ExpiredDocumentsCleaner : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly ExpirationConfiguration _configuration;

        private static readonly ILog Log = LogManager.GetLogger(typeof(ExpiredDocumentsCleaner));

        private const string DocumentsByExpiration = "DocumentsByExpiration";

        private readonly Timer _timer;
        private readonly object _locker = new object();
        private readonly byte[] _timerCurrentTimeBuffer = new byte[sizeof(long)];

        private ExpiredDocumentsCleaner(DocumentDatabase database, ExpirationConfiguration configuration)
        {
            _database = database;
            _configuration = configuration;

            var deleteFrequencyInSeconds = _configuration.DeleteFrequencySeconds ?? 300;
            Log.Info($"Initialized expired document cleaner, will check for expired documents every {deleteFrequencyInSeconds} seconds");
            var period = TimeSpan.FromSeconds(deleteFrequencyInSeconds);
            _timer = new Timer(TimerCallback, null, period, period);
        }

        public static ExpiredDocumentsCleaner LoadConfigurations(DocumentDatabase database)
        {
            DocumentsOperationContext context;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var configuration = database.DocumentsStorage.Get(context, Constants.Expiration.RavenExpirationConfiguration);
                if (configuration == null)
                    return null;

                try
                {
                    var expirationConfiguration = JsonDeserialization.ExpirationConfiguration(configuration.Data);
                    if (expirationConfiguration.Active == false)
                        return null;

                    return new ExpiredDocumentsCleaner(database, expirationConfiguration);
                }
                catch (Exception e)
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug($"Cannot enable expired documents cleaner as the configuration document {Constants.Expiration.RavenExpirationConfiguration} is not valid: " + configuration.Data, e);
                    return null;
                }
            }
        }
       
        public void TimerCallback(object state)
        {
            if (_database.DatabaseShutdown.IsCancellationRequested)
                return;

            if (Monitor.TryEnter(_locker) == false)
                return;

            try
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("Trying to find expired documents to delete");

                var sliceWriter = new SliceWriter(_timerCurrentTimeBuffer);
                sliceWriter.WriteBigEndian(SystemTime.UtcNow.Ticks);
                var currentTime = sliceWriter.CreateSlice();

                var toDelete = new HashSet<string>();

                DocumentsOperationContext context;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var tree = tx.InnerTransaction.CreateTree(DocumentsByExpiration);
                    using (var it = tree.Iterate())
                    {
                        if (it.Seek(Slice.BeforeAllKeys) == false)
                            return;

                        while (it.CurrentKey.Compare(currentTime) < 1)
                        {
                            using (var multiIt = tree.MultiRead(it.CurrentKey))
                            {
                                if (multiIt.Seek(Slice.BeforeAllKeys))
                                {
                                    do
                                    {
                                        // TODO: Maybe we should improve the ToString to use the same buffer instead of allocating new one
                                        toDelete.Add(multiIt.CurrentKey.ToString());
                                    } while (multiIt.MoveNext());
                                }
                            }

                            if (it.DeleteCurrentAndMoveNext() == false)
                                break;
                        }
                    }

                    if (toDelete.Count > 0)
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug($"Deleting {toDelete.Count} expired documents: [{string.Join(", ", toDelete)}]");

                        foreach (var key in toDelete)
                        {
                            if (_database.DatabaseShutdown.IsCancellationRequested)
                            {
                                if (Log.IsDebugEnabled)
                                    Log.Debug($"Stop deleting {toDelete.Count} expired documents at {key} because of database was shutdown");
                                return;
                            }

                            var deleted = _database.DocumentsStorage.Delete(context, key, null);

                            if (Log.IsDebugEnabled && deleted == false)
                                Log.Debug($"Tried to delete expired document '{key}' but document was not found.");
                        }
                    }

                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Error when trying to find expired documents", e);
            }
            finally
            {
                Monitor.Exit(_locker);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            _timer.Dispose();
        }

        public void Put(DocumentsOperationContext context, string originalCollectionName, string key, long newEtagBigEndian, BlittableJsonReaderObject document)
        {
            string expirationDate;
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Metadata, out metadata) == false ||
                metadata.TryGet(Constants.Expiration.RavenExpirationDate, out expirationDate) == false)
                return;

            DateTime date;
            if (DateTime.TryParseExact(expirationDate, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out date) == false)
                throw new InvalidOperationException($"The expiration date format is not valid: '{expirationDate}'. Use the following format: {SystemTime.UtcNow.ToString("O")}");

            if (SystemTime.UtcNow >= date)
                throw new InvalidOperationException($"Cannot put an expired document. Expired on: {date.ToString("O")}");

            var buffer = context.GetManagedBuffer();
            var sliceWriter = new SliceWriter(buffer);
            sliceWriter.WriteBigEndian(date.Ticks);
            var slice = sliceWriter.CreateSlice(sizeof(long));

            var tree = context.Transaction.InnerTransaction.CreateTree(DocumentsByExpiration);
            tree.MultiAdd(slice, key);
        }
    }
}