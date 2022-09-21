using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.NotificationCenter.Notifications;

namespace Raven.Server.NotificationCenter.BackgroundWork;

public class DatabaseStatsSender : AbstractDatabaseStatsSender
{
    private readonly DocumentDatabase _database;

    public DatabaseStatsSender([NotNull] DocumentDatabase database, DatabaseNotificationCenter notificationCenter)
        : base(database.Name, notificationCenter, database.DatabaseShutdown)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override ValueTask<NotificationCenterDatabaseStats> GetStatsAsync() => new(GetStats(_database));

    public static NotificationCenterDatabaseStats GetStats(DocumentDatabase database)
    {
        DateTime? lastIndexingErrorTime = null;

        var indexes = database.IndexStore.GetIndexes().ToList();
        var needsServerContext = indexes.Any(x => x.Definition.HasCompareExchange);
        var staleIndexes = new List<string>();
        var countOfIndexingErrors = 0L;

        using (var context = QueryOperationContext.Allocate(database, needsServerContext))
        using (context.OpenReadTransaction())
        {
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var index in indexes)
            {
                if (index.IsStale(context))
                    staleIndexes.Add(index.Name);

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

            return new NotificationCenterDatabaseStats
            {
                CountOfConflicts = database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context.Documents),
                CountOfDocuments = database.DocumentsStorage.GetNumberOfDocuments(context.Documents),
                CountOfIndexes = indexes.Count,
                CountOfStaleIndexes = staleIndexes.Count,
                StaleIndexes = staleIndexes.ToArray(),
                CountOfIndexingErrors = countOfIndexingErrors,
                LastEtag = DocumentsStorage.ReadLastEtag(context.Documents.Transaction.InnerTransaction),
                GlobalChangeVector = DocumentsStorage.GetDatabaseChangeVector(context.Documents),
                LastIndexingErrorTime = lastIndexingErrorTime,
                Collections = database.DocumentsStorage.GetCollections(context.Documents)
                    .ToDictionary(x => x.Name, x => new DatabaseStatsChanged.ModifiedCollection(x.Name, x.Count, database.DocumentsStorage.GetLastDocumentChangeVector(context.Documents.Transaction.InnerTransaction, context.Documents, x.Name)))
            };
        }
    }
}
