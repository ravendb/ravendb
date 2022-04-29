using System;
using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.Documents.Sharding;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedIndexHasChangedController : AbstractIndexHasChangedController
{
    private readonly ShardedDatabaseContext _context;

    public ShardedIndexHasChangedController([NotNull] ShardedDatabaseContext context)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override IndexInformationHolder GetIndex(string name) => _context.Indexes.GetIndex(name);

    protected override RavenConfiguration GetDatabaseConfiguration() => _context.Configuration;
}
