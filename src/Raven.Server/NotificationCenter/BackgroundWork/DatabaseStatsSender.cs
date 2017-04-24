using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.NotificationCenter.BackgroundWork
{
    public class DatabaseStatsSender : BackgroundWorkBase
    {
        private readonly DocumentDatabase _database;
        private readonly NotificationCenter _notificationCenter;

        private Stats _latest;

        public DatabaseStatsSender(DocumentDatabase database, NotificationCenter notificationCenter)
            : base(database.Name, database.DatabaseShutdown)
        {
            _database = database;
            _notificationCenter = notificationCenter;
        }

        protected override async Task Run()
        {
            while (CancellationToken.IsCancellationRequested == false)
            {
                try
                {
                    try
                    {
                        await Task.Delay(_notificationCenter.Options.DatabaseStatsThrottle, CancellationToken);

                    }
                    catch (Exception)
                    {
                        // can happen if there is an invalid timespan
                        return;
                    }
                    if (_database.DatabaseShutdown.IsCancellationRequested)
                        break;

                    Stats current;

                    DocumentsOperationContext context;
                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                    using (context.OpenReadTransaction())
                    {
                        var indexes = _database.IndexStore.GetIndexes().ToList();
                        var staleIndexes = 0;
                        var countOfIndexingErrors = 0L;

                        // ReSharper disable once LoopCanBeConvertedToQuery
                        foreach (var index in indexes)
                        {
                            if (index.IsStale(context))
                                staleIndexes++;

                            countOfIndexingErrors += index.GetErrorCount();
                        }

                        current = new Stats
                        {
                            CountOfDocuments = _database.DocumentsStorage.GetNumberOfDocuments(context),
                            CountOfIndexes = indexes.Count,
                            CountOfStaleIndexes = staleIndexes,
                            CountOfIndexingErrors = countOfIndexingErrors,
                            LastEtag = DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction),
                        };

                        current.Collections = _database.DocumentsStorage.GetCollections(context)
                            .ToDictionary(x => x.Name, x => new DatabaseStatsChanged.ModifiedCollection(x.Name, x.Count, _database.DocumentsStorage.GetLastDocumentEtag(context, x.Name)));
                    }

                    if (_latest != null && _latest.Equals(current))
                        continue;

                    var modifiedCollections = _latest == null ? current.Collections.Values.ToList() : ExtractModifiedCollections(current);

                    _notificationCenter.Add(DatabaseStatsChanged.Create(current.CountOfDocuments, current.CountOfIndexes,
                        current.CountOfStaleIndexes, current.LastEtag, current.CountOfIndexingErrors, modifiedCollections));

                    _latest = current;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Error on sending database stats notification", e);
                }
            }
        }

        private List<DatabaseStatsChanged.ModifiedCollection> ExtractModifiedCollections(Stats current)
        {
            var result = new List<DatabaseStatsChanged.ModifiedCollection>();

            foreach (var collection in _latest.Collections)
            {
                DatabaseStatsChanged.ModifiedCollection stats;
                if (current.Collections.TryGetValue(collection.Key, out stats) == false)
                {
                    // collection deleted

                    result.Add(new DatabaseStatsChanged.ModifiedCollection(collection.Key, -1, -1));

                    continue;
                }

                if (collection.Value.Count != stats.Count || collection.Value.LastDocumentEtag != stats.LastDocumentEtag)
                    result.Add(current.Collections[collection.Key]);
            }

            foreach (var collection in current.Collections)
            {
                if (_latest.Collections.ContainsKey(collection.Key) == false)
                {
                    result.Add(collection.Value); // new collection
                }
            }

            return result;
        }

        private class Stats
        {
            public long CountOfDocuments;

            public long LastEtag;

            public int CountOfIndexes;

            public int CountOfStaleIndexes;

            public long CountOfIndexingErrors;

            public Dictionary<string, DatabaseStatsChanged.ModifiedCollection> Collections;

            public bool Equals(Stats other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return CountOfDocuments == other.CountOfDocuments &&
                       CountOfIndexes == other.CountOfIndexes &&
                       CountOfIndexingErrors == other.CountOfIndexingErrors &&
                       LastEtag == other.LastEtag &&
                       CountOfStaleIndexes == other.CountOfStaleIndexes &&
                       DictionaryExtensions.ContentEquals(Collections, other.Collections);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;

                var stats = obj as Stats;
                if (stats == null)
                    return false;

                return Equals((Stats)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = CountOfDocuments.GetHashCode();
                    hashCode = (hashCode * 397) ^ CountOfIndexes.GetHashCode();
                    hashCode = (hashCode * 397) ^ LastEtag.GetHashCode();
                    hashCode = (hashCode * 397) ^ CountOfIndexingErrors.GetHashCode();
                    hashCode = (hashCode * 397) ^ CountOfStaleIndexes.GetHashCode();
                    hashCode = (hashCode * 397) ^ (Collections != null ? Collections.GetHashCode() : 0);
                    return hashCode;
                }
            }
        }
    }
}