﻿using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Sparrow.Utils;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedIndexDeleteProcessor : AbstractIndexDeleteProcessor
{
    private readonly ShardedDatabaseContext _context;

    public ShardedIndexDeleteProcessor([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        : base(serverStore)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override string GetDatabaseName() => _context.DatabaseName;

    protected override IndexDefinitionBase GetIndexDefinition(string name)
    {
        if (_context.Indexes.TryGetIndexDefinition(name, out var definition))
            return definition;

        return null;
    }

    protected override ValueTask CreateIndexAsync(IndexDefinition definition, string raftRequestId)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Here we want to use IndexCreateProcessor");
        throw new System.NotImplementedException();
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await ServerStore.Cluster.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
    }
}
