using System;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedDatabaseIndexDeleteController : DatabaseIndexDeleteController
{
    private readonly ShardedDocumentDatabase _database;

    public ShardedDatabaseIndexDeleteController([NotNull] ShardedDocumentDatabase database)
        : base(database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override string GetDatabaseName() => _database.ShardedDatabaseName;
}
