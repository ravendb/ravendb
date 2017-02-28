using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.NotificationCenter.BackgroundWork
{
    public class DatabaseStatsNotifier
    {
        private readonly DocumentDatabase _database;
        private readonly TimeSpan _delay = TimeSpan.FromSeconds(5);
        private Stats _latest = null;

        public DatabaseStatsNotifier(DocumentDatabase database)
        {
            _database = database;
        }

        public async Task DatabaseStatsChangedNotificationSender()
        {
            while (_database.DatabaseShutdown.IsCancellationRequested == false)
            {
                await Task.Delay(_delay);

                Stats current;

                DocumentsOperationContext context;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    var indexes = _database.IndexStore.GetIndexes().ToList();
                    int staleIndexes = 0;

                    foreach (var index in indexes)
                    {
                        if (index.IsStale(context))
                            staleIndexes++;
                    }

                    current = new Stats
                    {
                        CountOfDocuments = _database.DocumentsStorage.GetNumberOfDocuments(context),
                        CountOfIndexes = indexes.Count,
                        CountOfStaleIndexes = staleIndexes,
                        Collections = _database.DocumentsStorage.GetCollections(context).ToList()
                    };
                }

                if (_latest == null || _latest.Equals(current) == false)
                {
                    List<DocumentsStorage.CollectionStats> modifiedCollections = null;

                    if (_latest == null)
                        modifiedCollections = current.Collections.ToList();
                    else
                    {
                        if (current.Collections == _latest.Collections)
                        {
                            //for (int i = 0; i < UPPER; i++)
                            //{

                            //}
                        }
                    }

                    DatabaseStatsChanged.Create(current.CountOfDocuments, current.CountOfIndexes,
                        current.CountOfStaleIndexes, modifiedCollections);

                    _latest = current;
                }
            }
        }

        private class Stats : IEquatable<Stats>
        {
            public long CountOfDocuments;

            public int CountOfIndexes;

            public int CountOfStaleIndexes;

            public List<DocumentsStorage.CollectionStats> Collections;

            public bool Equals(Stats other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return CountOfDocuments == other.CountOfDocuments &&
                    CountOfIndexes == other.CountOfIndexes &&
                    CountOfStaleIndexes == other.CountOfStaleIndexes && 
                    Equals(Collections, other.Collections);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((Stats) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = CountOfDocuments.GetHashCode();
                    hashCode = (hashCode * 397) ^ CountOfIndexes.GetHashCode();
                    hashCode = (hashCode * 397) ^ CountOfStaleIndexes.GetHashCode();
                    hashCode = (hashCode * 397) ^ (Collections != null ? Collections.GetHashCode() : 0);
                    return hashCode;
                }
            }
        }
    }
}