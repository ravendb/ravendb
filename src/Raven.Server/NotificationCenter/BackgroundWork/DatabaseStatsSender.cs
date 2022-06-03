using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;

namespace Raven.Server.NotificationCenter.BackgroundWork
{
    public class DatabaseStatsSender : BackgroundWorkBase
    {
        private readonly DocumentDatabase _database;
        private readonly DatabaseNotificationCenter _notificationCenter;

        private Stats _latest;

        public DatabaseStatsSender(DocumentDatabase database, DatabaseNotificationCenter notificationCenter)
            : base(database.Name, database.DatabaseShutdown)
        {
            _database = database;
            _notificationCenter = notificationCenter;
        }

        protected override async Task DoWork()
        {
            await WaitOrThrowOperationCanceled(_notificationCenter.Options.DatabaseStatsThrottle);

            Stats current;
            DateTime? lastIndexingErrorTime = null;

            var indexes = _database.IndexStore.GetIndexes().ToList();
            var needsServerContext = indexes.Any(x => x.Definition.HasCompareExchange);
            var staleIndexes = 0;
            var countOfIndexingErrors = 0L;

            using (var context = QueryOperationContext.Allocate(_database, needsServerContext))
            using (context.OpenReadTransaction())
            {
                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (var index in indexes)
                {
                    if (index.IsStale(context))
                        staleIndexes++;

                    var errorCount = index.GetErrorCount();

                    if (errorCount > 0)
                    {
                        var lastError = index.GetLastIndexingErrorTime();
                        if (lastError != null)
                        {
                            if (lastIndexingErrorTime == null || lastError > lastIndexingErrorTime)
                            {
                                lastIndexingErrorTime = lastError;
                            }
                        }
                    }
                    countOfIndexingErrors += errorCount;
                }

                current = new Stats
                {
                    CountOfConflicts = _database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context.Documents),
                    CountOfDocuments = _database.DocumentsStorage.GetNumberOfDocuments(context.Documents),
                    CountOfIndexes = indexes.Count,
                    CountOfStaleIndexes = staleIndexes,
                    CountOfIndexingErrors = countOfIndexingErrors,
                    LastEtag = DocumentsStorage.ReadLastEtag(context.Documents.Transaction.InnerTransaction),
                    GlobalChangeVector = DocumentsStorage.GetDatabaseChangeVector(context.Documents)
                };
                current.Collections = _database.DocumentsStorage.GetCollections(context.Documents)
                    .ToDictionary(x => x.Name, x => new DatabaseStatsChanged.ModifiedCollection(x.Name, x.Count, _database.DocumentsStorage.GetLastDocumentChangeVector(context.Documents.Transaction.InnerTransaction, context.Documents, x.Name)));
            }

            if (_latest != null && _latest.Equals(current))
                return;

            var modifiedCollections = _latest == null ? current.Collections.Values.ToList() : ExtractModifiedCollections(current);

            _notificationCenter.Add(DatabaseStatsChanged.Create(
                _database.Name,
                current.CountOfConflicts,
                current.CountOfDocuments,
                current.CountOfIndexes,
                current.CountOfStaleIndexes,
                current.GlobalChangeVector,
                current.LastEtag,
                current.CountOfIndexingErrors,
                lastIndexingErrorTime,
                modifiedCollections));

            _latest = current;
        }

        private List<DatabaseStatsChanged.ModifiedCollection> ExtractModifiedCollections(Stats current)
        {
            var result = new List<DatabaseStatsChanged.ModifiedCollection>();

            foreach (var collection in _latest.Collections)
            {
                if (current.Collections.TryGetValue(collection.Key, out DatabaseStatsChanged.ModifiedCollection stats) == false)
                {
                    // collection deleted

                    result.Add(new DatabaseStatsChanged.ModifiedCollection(collection.Key, -1, null));

                    continue;
                }

                if (collection.Value.Count != stats.Count || collection.Value.LastDocumentChangeVector != stats.LastDocumentChangeVector)
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
            public long CountOfConflicts;

            public long CountOfDocuments;

            public long LastEtag;

            public int CountOfIndexes;

            public int CountOfStaleIndexes;

            public long CountOfIndexingErrors;

            public string GlobalChangeVector;

            public Dictionary<string, DatabaseStatsChanged.ModifiedCollection> Collections;

            public bool Equals(Stats other)
            {
                if (ReferenceEquals(null, other))
                    return false;
                if (ReferenceEquals(this, other))
                    return true;
                return CountOfConflicts == other.CountOfConflicts &&
                       CountOfDocuments == other.CountOfDocuments &&
                       CountOfIndexes == other.CountOfIndexes &&
                       CountOfIndexingErrors == other.CountOfIndexingErrors &&
                       LastEtag == other.LastEtag &&
                       CountOfStaleIndexes == other.CountOfStaleIndexes &&
                       GlobalChangeVector == other.GlobalChangeVector &&
                       DictionaryExtensions.ContentEquals(Collections, other.Collections);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (ReferenceEquals(this, obj))
                    return true;

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
                    hashCode = (hashCode * 397) ^ GlobalChangeVector.GetHashCode();
                    hashCode = (hashCode * 397) ^ (Collections != null ? Collections.GetHashCode() : 0);
                    return hashCode;
                }
            }
        }
    }
}
