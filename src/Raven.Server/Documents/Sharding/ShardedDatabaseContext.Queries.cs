using Raven.Server.Documents.Sharding.Queries;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedQueryRunner QueryRunner;
}
