using System;
using JetBrains.Annotations;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public AbstractDatabaseLoggersContext Loggers;

    private sealed class ShardedLoggersContext : AbstractDatabaseLoggersContext
    {
        private readonly ShardedDatabaseContext _databaseContext;

        public ShardedLoggersContext([NotNull] ShardedDatabaseContext databaseContext)
        {
            _databaseContext = databaseContext ?? throw new ArgumentNullException(nameof(databaseContext));
        }

        protected override RavenLogger CreateLogger(Type type) => RavenLogManager.Instance.GetLoggerForDatabase(type, _databaseContext);
    }
}
