//-----------------------------------------------------------------------
// <copyright file="ExpiredDocumentsCleaner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Plugins;

namespace Raven.Database.Bundles.Expiration
{
    [InheritedExport(typeof(IStartupTask))]
    [ExportMetadata("Bundle", "DocumentExpiration")]
    public class ExpiredDocumentsCleaner : IStartupTask, IDisposable
    {
        public const string RavenDocumentsByExpirationDate = "Raven/DocumentsByExpirationDate";
        private readonly ILog logger = LogManager.GetCurrentClassLogger();
        public DocumentDatabase Database { get; set; }

        public void Execute(DocumentDatabase database)
        {
            Database = database;

            var indexDefinition = database.Indexes.GetIndexDefinition(RavenDocumentsByExpirationDate);
            if (indexDefinition == null)
            {
                database.Indexes.PutIndex(RavenDocumentsByExpirationDate,
                                  new IndexDefinition
                                  {
                                      Map =
                                          @"
    from doc in docs
    let expiry = doc[""@metadata""][""Raven-Expiration-Date""]
    where expiry != null
    select new { Expiry = expiry }
"
                                  });
            }

            var deleteFrequencyInSeconds = database.Configuration.GetConfigurationValue<int>("Raven/Expiration/DeleteFrequencySeconds") ?? 300;
            logger.Info("Initialized expired document cleaner, will check for expired documents every {0} seconds",
                        deleteFrequencyInSeconds);

            timer = database.TimerManager.NewTimer(state => TimerCallback(), TimeSpan.FromSeconds(deleteFrequencyInSeconds), TimeSpan.FromSeconds(deleteFrequencyInSeconds));
        }

        private object locker = new object();
        private Timer timer;

        public bool TimerCallback()
        {
            if (Database.Disposed)
            {
                return false;
            }

            if (Monitor.TryEnter(locker) == false)
                return false;

            try
            {
                DateTime currentTime = SystemTime.UtcNow;
                string nowAsStr = currentTime.GetDefaultRavenFormat();
                if (logger.IsDebugEnabled)
                    logger.Debug("Trying to find expired documents to delete");
                var query = "Expiry:[* TO " + nowAsStr + "]";

                var list = new List<string>();
                int start = 0;
                while (true)
                {
                    const int pageSize = 1024;

                    QueryResultWithIncludes queryResult;
                    using (var cts = new CancellationTokenSource())
                    using (Database.DisableAllTriggersForCurrentThread())
                    using (cts.TimeoutAfter(TimeSpan.FromMinutes(5)))
                    {
                        queryResult = Database.Queries.Query(RavenDocumentsByExpirationDate, new IndexQuery
                        {
                            Start = start,
                            PageSize = pageSize,
                            Cutoff = currentTime,
                            Query = query,
                            FieldsToFetch = new[] { "__document_id" }
                        }, cts.Token);
                    }

                    if (queryResult.Results.Count == 0)
                        break;

                    list.AddRange(queryResult.Results.Select(result => result.Value<string>("__document_id")).Where(x => string.IsNullOrEmpty(x) == false));

                    if (queryResult.Results.Count < pageSize)
                        break;

                    start += pageSize;

                    if (Database.Disposed)
                        return false;
                }

                if (list.Count == 0)
                    return true;

                if (logger.IsDebugEnabled)
                    logger.Debug(
                    () => string.Format("Deleting {0} expired documents: [{1}]", list.Count, string.Join(", ", list)));

                foreach (var id in list)
                {
                    Database.Documents.Delete(id, null, null);

                    if (Database.Disposed)
                        return false;
                }
            }
            catch (Exception e)
            {
                logger.ErrorException("Error when trying to find expired documents", e);
            }
            finally
            {
                Monitor.Exit(locker);
            }
            return true;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            if(timer != null)
                Database.TimerManager.ReleaseTimer(timer);
            timer = null;
        }
    }
}
