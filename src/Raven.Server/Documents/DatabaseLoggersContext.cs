using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents;

public class DatabaseLoggersContext : AbstractDatabaseLoggersContext
{
    private readonly DocumentDatabase _database;

    public DatabaseLoggersContext([NotNull] DocumentDatabase database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override RavenLogger CreateLogger(Type type) => RavenLogManager.Instance.GetLoggerForDatabase(type, _database);
}

public abstract class AbstractDatabaseLoggersContext
{
    private FrozenDictionary<Type, RavenLogger> _loggers = new Dictionary<Type, RavenLogger>().ToFrozenDictionary();

    protected abstract RavenLogger CreateLogger(Type type);

    public RavenLogger GetLogger<T>() => GetLogger(typeof(T));

    public RavenLogger GetLogger(Type type)
    {
        if (_loggers.TryGetValue(type, out var logger))
            return logger;

        logger = CreateLogger(type);

        var loggers = new Dictionary<Type, RavenLogger>(_loggers)
        {
            [type] = logger
        };

        _loggers = loggers.ToFrozenDictionary();

        return logger;
    }
}
