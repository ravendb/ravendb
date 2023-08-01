using System;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding;

namespace Raven.Server.Documents.Indexes.Sharding;

public sealed class ShardedDatabaseIndexStateController : DatabaseIndexStateController
{
    private readonly ShardedDocumentDatabase _database;

    public ShardedDatabaseIndexStateController([NotNull] ShardedDocumentDatabase database)
        : base(database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override string GetDatabaseName() => _database.ShardedDatabaseName;
}
