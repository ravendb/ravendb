using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Nest;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;
using Tests.Infrastructure.ConnectionString;
using Xunit;
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
            
        }

        protected ElasticSearchEtlConfiguration SetupElasticEtl(DocumentStore store, string script, IEnumerable<string> collections = null, bool applyToAllDocuments = false,
            global::Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication authentication = null, [CallerMemberName] string caller = null, string configurationName = null, string transformationName = null, string[] nodes = null)
        {
            var connectionStringName = $"{store.Database}@{store.Urls.First()} to ELASTIC";

            var config = new ElasticSearchEtlConfiguration
            {
                Name = configurationName ?? connectionStringName,
                ConnectionStringName = connectionStringName,
                ElasticIndexes =
                {
                    new ElasticSearchIndex {IndexName = $"Orders", DocumentIdProperty = "Id"},
                    new ElasticSearchIndex {IndexName = $"OrderLines", DocumentIdProperty = "OrderId"},
                    new ElasticSearchIndex {IndexName = $"Users", DocumentIdProperty = "UserId"},
                },
                Transforms =
                {
                    new Transformation
                    {
                        Name = transformationName ?? $"ETL : {connectionStringName}",
                        Collections = new List<string>(collections),
                        Script = script,
                        ApplyToAllDocuments = applyToAllDocuments
                    }
                }
            };

            AddEtl(store, config, new ElasticSearchConnectionString { Name = connectionStringName, Nodes = nodes ?? ElasticSearchTestNodes.Instance.VerifiedNodes.Value, Authentication = authentication });

            return config;
        }

        protected IDisposable GetElasticClient(out ElasticClient client)
        {
            ElasticClient localClient;

            ConcurrentEsEtlTests.Wait();

            try
            {
                localClient = client = ElasticSearchHelper.CreateClient(new ElasticSearchConnectionString { Nodes = ElasticSearchTestNodes.Instance.VerifiedNodes.Value });

                CleanupIndexes(localClient);
            }
            catch
            {
                ConcurrentEsEtlTests.Release();
                throw;
            }

            return new DisposableAction(() =>
            {
                try
                {
                    CleanupIndexes(localClient);
                }
                finally 
                {
                    ConcurrentEsEtlTests.Release();
                }
            });
        }

        protected void CleanupIndexes(ElasticClient client)
        {
            client.Indices.Refresh();

            var response = client.Indices.Delete(Indices.All);

            if (response.IsValid == false)
            {
                if (response.ServerError?.Status == 404)
                    return;

                throw new InvalidOperationException($"Failed to cleanup indexes: {response.ServerError}", response.OriginalException);
            }

            client.Indices.Refresh();
        }

        protected void AssertEtlDone(ManualResetEventSlim etlDone, TimeSpan timeout, string databaseName, ElasticSearchEtlConfiguration config)
        {
            if (etlDone.Wait(timeout) == false)
            {
                TryGetLoadError(databaseName, config, out var loadError);
                TryGetTransformationError(databaseName, config, out var transformationError);

                Assert.True(false, $"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
            }
        }
    }
}
