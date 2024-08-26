using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Background;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter.BackgroundWork;

public abstract class AbstractDatabaseStatsSender : BackgroundWorkBase
{
    private readonly string _databaseName;
    private readonly AbstractDatabaseNotificationCenter _notificationCenter;

    private NotificationCenterDatabaseStats _latest;

    protected AbstractDatabaseStatsSender([NotNull] string databaseName, AbstractDatabaseNotificationCenter notificationCenter, CancellationToken shutdown)
        : base(databaseName, RavenLogManager.Instance.GetLoggerForDatabase<AbstractDatabaseStatsSender>(databaseName), shutdown)
    {
        _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        _notificationCenter = notificationCenter;
    }

    protected abstract ValueTask<NotificationCenterDatabaseStats> GetStatsAsync();

    protected override async Task DoWork()
    {
        await WaitOrThrowOperationCanceled(_notificationCenter.Options.DatabaseStatsThrottle);

        var current = await GetStatsAsync();

        if (_latest != null && _latest.Equals(current))
            return;

        var modifiedCollections = _latest == null ? current.Collections.Values.ToList() : ExtractModifiedCollections(current);

        _notificationCenter.Add(DatabaseStatsChanged.Create(
            _databaseName,
            current.CountOfConflicts,
            current.CountOfDocuments,
            current.CountOfIndexes,
            current.CountOfStaleIndexes,
            current.GlobalChangeVector,
            current.LastEtag,
            current.CountOfIndexingErrors,
            current.LastIndexingErrorTime,
            modifiedCollections));

        _latest = current;
    }

    private List<DatabaseStatsChanged.ModifiedCollection> ExtractModifiedCollections(NotificationCenterDatabaseStats current)
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
}
