using System.Threading;
using Raven.Server.Config;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter;

public abstract class AbstractDatabaseNotificationCenter : AbstractNotificationCenter
{
    public readonly string Database;

    public readonly Paging Paging;
    public readonly ConflictRevisionsExceeded ConflictRevisionsExceeded;
    public readonly TombstoneNotifications TombstoneNotifications;
    public readonly Indexing Indexing;
    public readonly RequestLatency RequestLatency;
    public readonly EtlNotifications EtlNotifications;
    public readonly QueueSinkNotifications QueueSinkNotifications;
    public readonly SlowWriteNotifications SlowWrites;

    protected AbstractDatabaseNotificationCenter(ServerStore serverStore, string database, RavenConfiguration configuration, CancellationToken shutdown)
        : this(serverStore.NotificationCenter.Storage.GetStorageFor(database), database, configuration, shutdown)
    {
    }

    protected AbstractDatabaseNotificationCenter(NotificationsStorage notificationsStorage, string database, RavenConfiguration configuration, CancellationToken shutdown)
        : base(notificationsStorage, configuration, RavenLogManager.Instance.GetLoggerForDatabase<AbstractDatabaseNotificationCenter>(database))
    {
        Database = database;
        Paging = new Paging(this);
        ConflictRevisionsExceeded = new ConflictRevisionsExceeded(this);
        TombstoneNotifications = new TombstoneNotifications(this);
        Indexing = new Indexing(this);
        RequestLatency = new RequestLatency(this);
        EtlNotifications = new EtlNotifications(this);
        SlowWrites = new SlowWriteNotifications(this);
        QueueSinkNotifications = new QueueSinkNotifications(this);

        PostponedNotificationSender = new PostponedNotificationsSender(database, Storage, Watchers, RavenLogManager.Instance.GetLoggerForDatabase<PostponedNotificationsSender>(database), shutdown);
    }

    protected override PostponedNotificationsSender PostponedNotificationSender { get; }

    public override void Dispose()
    {
        Paging?.Dispose();
        ConflictRevisionsExceeded?.Dispose();
        Indexing?.Dispose();
        RequestLatency?.Dispose();
        SlowWrites?.Dispose();

        base.Dispose();
    }
}
