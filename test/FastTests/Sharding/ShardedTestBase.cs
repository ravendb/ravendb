using System;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
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
                options?.ModifyDatabaseRecord?.Invoke(r);            };
            //shardedOptions.RunInMemory = false;
            return GetDocumentStore(shardedOptions, caller);
        }

        public new void WaitForUserToContinueTheTest(IDocumentStore documentStore, bool debug = true, string database = null, X509Certificate2 clientCert = null)
        {
            var db = database ?? $"{documentStore.Database}$0";
            RavenTestBase.WaitForUserToContinueTheTest(documentStore, debug, db, clientCert);
        }
    }
}
