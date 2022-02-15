using System.Runtime.CompilerServices;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Sparrow.Utils;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    [Trait("Category", "Sharding")]
    public abstract class ShardedTestBase : RavenTestBase
    {
        protected ShardedTestBase(ITestOutputHelper output) : base(output)
        {
        }

        protected IDocumentStore GetShardedDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            var shardedOptions = options ?? new Options();
            shardedOptions.ModifyDatabaseRecord += r =>
            {
                r.Shards = new[]
                {
                    new DatabaseTopology(), 
                    new DatabaseTopology(), 
                    new DatabaseTopology(),
                };
            };
            shardedOptions.ModifyDocumentStore = s => s.Conventions.OperationStatusFetchMode = OperationStatusFetchMode.Polling;
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,"remove above after changes api is working");
            //shardedOptions.RunInMemory = false;
            return GetDocumentStore(shardedOptions, caller);
        }
    }
}
