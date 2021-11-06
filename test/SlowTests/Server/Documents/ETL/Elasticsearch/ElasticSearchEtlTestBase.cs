using System;
using System.Threading;
using Nest;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;
using Tests.Infrastructure.ConnectionString;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.ElasticSearch
{
    public class ElasticSearchEtlTestBase : EtlTestBase
    {
        // we're using a single instance of ES for all tests which can run in parallel
        // there is no notion of a database in ES and we use the same index names in the tests
        // so we are limiting the number of concurrent tests running against ES to avoid data conflicts
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/_mapping_concepts_across_sql_and_elasticsearch.html

        private static readonly SemaphoreSlim ConcurrentEsEtlTests = new SemaphoreSlim(1, 1);
        public ElasticSearchEtlTestBase(ITestOutputHelper output) : base(output)
        {
            ConcurrentEsEtlTests.Wait();
        }

        protected IDisposable GetElasticClient(out ElasticClient client)
        {
            var localClient = client = ElasticSearchHelper.CreateClient(new ElasticSearchConnectionString { Nodes = ElasticSearchTestNodes.Instance.VerifiedNodes.Value });

            CleanupIndexes(localClient);

            return new DisposableAction(() =>
            {
                CleanupIndexes(localClient);
            });
        }

        protected void CleanupIndexes(ElasticClient client)
        {
            var response = client.Indices.Delete(Indices.All);
        }

        public override void Dispose()
        {
            base.Dispose();

            ConcurrentEsEtlTests.Release();
        }
    }
}
