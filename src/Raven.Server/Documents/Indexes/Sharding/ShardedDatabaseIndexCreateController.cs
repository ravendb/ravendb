using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Sharding;

namespace Raven.Server.Documents.Indexes.Sharding;

public sealed class ShardedDatabaseIndexCreateController : DatabaseIndexCreateController
{
    private readonly ShardedDocumentDatabase _database;

    public ShardedDatabaseIndexCreateController([NotNull] ShardedDocumentDatabase database)
        : base(database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    protected override string GetDatabaseName() => _database.ShardedDatabaseName;

    protected override void ValidateAutoIndex(IndexDefinitionBaseServerSide definition)
    {
        if (definition.DeploymentMode == IndexDeploymentMode.Rolling)
            throw new NotSupportedInShardingException("Rolling index deployment for a sharded database is currently not supported");

        base.ValidateAutoIndex(definition);
    }
}
