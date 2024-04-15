using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Utils;
using static Raven.Server.Documents.DatabasesLandlord;

namespace Raven.Server.Documents;

public class DatabaseRaftIndexNotifications : AbstractRaftIndexNotifications
{
    private readonly Queue<DatabaseNotification> _recentNotifications = new Queue<DatabaseNotification>();

    private readonly RachisLogIndexNotifications _clusterStateMachineLogIndexNotifications;

    public DatabaseRaftIndexNotifications(RachisLogIndexNotifications clusterStateMachineLogIndexNotifications, CancellationToken token) : base(token)
    {
        _clusterStateMachineLogIndexNotifications = clusterStateMachineLogIndexNotifications;
    }

    protected override string PrintLastNotifications()
    {
        var notifications = _recentNotifications.ToArray();
        var builder = new StringBuilder(notifications.Length);
        foreach (var notification in notifications)
        {
            builder
                .Append("Index: ")
                .Append(notification.Index)
                .Append(". Type: ")
                .Append(notification.Type)
                .Append(". Exception: ")
                .Append(notification.Exception)
                .AppendLine();
        }
        return builder.ToString();
    }

    private void RecordNotification(DatabaseNotification notification)
    {
        _recentNotifications.Enqueue(notification);
        while (_recentNotifications.Count > 50)
            _recentNotifications.TryDequeue(out _);
    }

    public void NotifyListenersAbout(DatabaseNotification notification)
    {
        RecordNotification(notification);

        long index = notification.Index;
        Exception e = notification.Exception;

        NotifyListenersInternal(index, e);
    }

    internal override ConcurrentDictionary<long, TaskCompletionSource<object>> GetTasksDictionary()
    {
        return _clusterStateMachineLogIndexNotifications.GetTasksDictionary();
    }
}

public class DatabaseNotification
{
    public long Index;
    public Exception Exception;
    public DatabaseNotificationChangeType Type;
}

public enum DatabaseNotificationChangeType
{
    StateChanged,
    ValueChanged,
    PendingClusterTransactions,
    ClusterTransactionCompleted,

    IndexStart,
    IndexUpdateSorters,
    IndexUpdateAnalyzers,
    AutoIndexStart,
    UpdateStaticIndex,
    DeleteIndex
}
