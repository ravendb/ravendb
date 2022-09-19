using Raven.Server.Documents.Sharding.NotificationCenter;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public readonly ShardedDatabaseNotificationCenter NotificationCenter;
}
