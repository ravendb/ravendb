using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Xunit.Abstractions;

namespace RachisTests.DatabaseCluster
{
    public class ShardedAtomicClusterReadWriteTests : AtomicClusterReadWriteTests
    {
        public ShardedAtomicClusterReadWriteTests(ITestOutputHelper output) : base(output)
        {
        }

        protected override DocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            options ??= new Options();
            var modifyDatabaseRecord = options.ModifyDatabaseRecord;
            options.ModifyDatabaseRecord = r =>
            {
                modifyDatabaseRecord?.Invoke(r);
                var databaseTopology = r.Topology ?? new DatabaseTopology();
                r.Topology = null;

                r.Shards = Enumerable.Range(0, 3)
                    .Select(_ => databaseTopology)
                    .ToArray();
            };
            return base.GetDocumentStore(options, caller);
        }
    }
}
