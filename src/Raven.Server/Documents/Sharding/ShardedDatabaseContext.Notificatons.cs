using Raven.Server.NotificationCenter;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public readonly ShardedDatabaseNotificationCenter NotificationCenter;

    public class ShardedDatabaseNotificationCenter : AbstractDatabaseNotificationCenter
    {
        public ShardedDatabaseNotificationCenter(ShardedDatabaseContext context)
            : base(context._serverStore, context.DatabaseName, context.Configuration, context.DatabaseShutdown)
        {
        }
    }
}
