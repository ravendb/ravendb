using System.Threading;
using Raven.Server.Config;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter;

public abstract class AbstractDatabaseNotificationCenter : AbstractNotificationCenter
{
    public readonly string Database;

    public readonly Paging Paging;
    public readonly Indexing Indexing;
    public readonly RequestLatency RequestLatency;
    public readonly EtlNotifications EtlNotifications;
    public readonly SlowWriteNotifications SlowWrites;

    protected AbstractDatabaseNotificationCenter(ServerStore serverStore, string database, RavenConfiguration configuration, CancellationToken shutdown)
        : base(serverStore.NotificationCenter.Storage.GetStorageFor(database), configuration, LoggingSource.Instance.GetLogger<DatabaseNotificationCenter>(database))
    {
        Database = database;
        Paging = new Paging(this);
        Indexing = new Indexing(this);
        RequestLatency = new RequestLatency(this);
        EtlNotifications = new EtlNotifications(this);
        SlowWrites = new SlowWriteNotifications(this);

        PostponedNotificationSender = new PostponedNotificationsSender(database, Storage, Watchers, shutdown);
    }

    protected override PostponedNotificationsSender PostponedNotificationSender { get; }

    public override void Dispose()
    {
        Paging?.Dispose();
        Indexing?.Dispose();
        RequestLatency?.Dispose();
        SlowWrites?.Dispose();

        base.Dispose();
    }
}
