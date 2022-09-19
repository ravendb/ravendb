using System;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.NotificationCenter.BackgroundWork;
using Raven.Server.NotificationCenter;

namespace Raven.Server.Documents.Sharding.NotificationCenter;

public class ShardedDatabaseNotificationCenter : AbstractDatabaseNotificationCenter
{
    private readonly ShardedDatabaseContext _context;

    public ShardedDatabaseNotificationCenter([NotNull] ShardedDatabaseContext context)
        : base(context.ServerStore, context.DatabaseName, context.Configuration, context.DatabaseShutdown)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    public override void Initialize()
    {
        BackgroundWorkers.Add(new ShardedDatabaseStatsSender(_context, this));

        base.Initialize();
    }
}
